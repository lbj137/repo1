#
# Copyright 2020 Akamai Technologies, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#include "GRPCAsyncServer.h"
#include "GenericAsyncHandler.h"
#include "asyncstreamer.grpc.pb.h"
#include "asyncstreamer.pb.h"
#include <atomic>
#include <thread>
#include <chrono>
#include <memory>

#define GRPC_Namespace asyncstreamer 

template <class T> 
using AsyncStreamHandlerBase = grpcserver::GenericAsyncHandler< 
    GRPC_Namespace::RequestStream, GRPC_Namespace::StreamingResponse, 
    grpcserver::AsyncStreamingWriter, T>; 
class AsyncStreamHandler : public AsyncStreamHandlerBase<AsyncStreamHandler> 
{ 
public:
    template<typename... Args> 
    AsyncStreamHandler(Args&&... t) : AsyncStreamHandlerBase<AsyncStreamHandler> 
        (std::forward<Args>(t)...) {} \
    HandlerStatus SendResponse(void* opaque_handle) override 
    {
        HandlerStatus status = ACTIVE; 
        const bool is_finished = --num_responses_remaining_ == 0;
        try {
            GRPC_Namespace::StreamingResponse response;
            response.set_response(request_.req_id() + 
                ": generic response number " + std::to_string(num_responses_remaining_));
            if (is_finished) {
                grpc::WriteOptions options;
                responder_->WriteAndFinish(response, options, grpc::Status::OK, opaque_handle);    
                status = FINISHED;
            }
            else if (num_responses_remaining_ % 2) {
                // simulate work by delaying 
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                responder_->Write(response, opaque_handle);    
            }
            else
                status = IDLE;
        }
        catch(std::runtime_error& err) {
            std::cerr << "Error occurred: " << err.what() << std::endl;
            status = FINISHED;
        }
        catch(...) {
            std::cerr << "Internal error occurred" << std::endl;
            status = FINISHED;
        }
        return status;
    }
private:
    int num_responses_remaining_ = 2000;
};

struct GrpcAsyncServerInfo {
    std::vector<std::shared_ptr<grpc::Service>> services_;
    std::vector<std::shared_ptr<grpcserver::AsyncServiceHandler>> handlers_;
};

void CreateGrpcAsyncServices(std::map<uint16_t /*port*/, GrpcAsyncServerInfo>& server_info_map) 
{
    #define ServiceRequester(RequestMethod, svc) \
        std::bind(RequestMethod, svc, \
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, \
        std::placeholders::_4, std::placeholders::_5, std::placeholders::_6)
    using GRPC_Generated_Service = GRPC_Namespace::StreamingService::AsyncService;
    auto* strm_svc = new GRPC_Generated_Service;
    auto& entry = server_info_map[9314];
    entry.handlers_.emplace_back(new AsyncStreamHandler(
        ServiceRequester(&GRPC_Generated_Service::
        RequestStreamMessages, strm_svc)));
    entry.services_.emplace_back(strm_svc);
}

int main(int, char**) {
   
    std::map<uint16_t, GrpcAsyncServerInfo> async_server_info_map; 
    CreateGrpcAsyncServices(async_server_info_map);
    std::vector<std::thread> async_server_threads;
    std::atomic_bool exit_bool(false);
    for (auto& info : async_server_info_map) {
        async_server_threads.emplace_back([info](
            const std::atomic_bool& should_exit) {
            const uint16_t port = info.first;
            grpcserver::GRPCAsyncServer server(
                port, info.second.services_,
                info.second.handlers_);
                server.Run(should_exit);
        }, std::cref(exit_bool));
    }
    std::cout << "Press Enter to exit..." << std::endl; 
    getchar(); 
    exit_bool = true;
    for (auto& thr : async_server_threads) 
        thr.join();
    std::cout << "Exited gracefully." << std::endl; 
  return 0;
}


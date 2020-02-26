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

#include <iostream>
#include <memory>
#include <string>

#include <grpc/grpc.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "asyncstreamer.grpc.pb.h"
#include "asyncstreamer.pb.h"

using namespace asyncstreamer;
using namespace grpc; 

class AsyncStreamerClient {
 public:
  AsyncStreamerClient(std::shared_ptr<Channel> channel)
      : stub_(StreamingService::NewStub(channel)) {
  }

  void OpenAndReadFromStream() {
    RequestStream req;
    timespec curr_time;
    curr_time.tv_nsec = 0;
    curr_time.tv_sec = 0;
    clock_gettime(CLOCK_REALTIME, &curr_time);
    req.set_req_id(std::string("request ") + std::to_string(curr_time.tv_nsec)); 
    ClientContext context;
    StreamingResponse response;
    std::unique_ptr<ClientReader<StreamingResponse>> reader(
        stub_->StreamMessages(&context, req));
    while (reader->Read(&response)) {
        std::cout << response.response() << std::endl;
    }
    Status status = reader->Finish();
    if (status.ok()) {
      std::cout << "StreamMessages rpc succeeded." << std::endl;
    } else {
      std::cout << "StreamMessages rpc failed." << status.error_message() << std::endl;
    }
  }
private:
     std::unique_ptr<StreamingService::Stub> stub_;
};

int main(int argc, char** argv) {
    AsyncStreamerClient client(
        grpc::CreateChannel("localhost:9314",
        grpc::InsecureChannelCredentials()));
    client.OpenAndReadFromStream(); 
    std::cout << "Client exiting." << std::endl;
    return 0;
}

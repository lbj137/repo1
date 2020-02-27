/*
 *
 * Copyright 2015 gRPC authors.
 * Copyright 2020 Akamai Technologies, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "GRPCAsyncServer.h"
#include <grpc/support/time.h>
#include <grpc++/security/server_credentials.h>
#include <thread>
#include <chrono>
#include <queue>
#include <sstream>
#include <fstream>

namespace grpcserver {

using namespace grpc;

GRPCAsyncServer::~GRPCAsyncServer() {
    Shutdown();
}

void
GRPCAsyncServer::Shutdown()
{
    server_->Shutdown(gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
        gpr_time_from_seconds(1, GPR_TIMESPAN)));
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
    // Drain the cq_ that was created
    void* tag = nullptr;
    bool ignored_ok(false);
    while (cq_->Next(&tag, &ignored_ok)) {
        CallData* cd = static_cast<CallData*>(tag);
        delete cd;
    }
}

void
GRPCAsyncServer::Run(const std::atomic_bool& should_exit)
{
    ServerBuilder builder;
    const auto server_credentials = grpc::InsecureServerCredentials();
    const std::string server_address =  std::string("0.0.0.0:") +
        std::to_string(port_);
    builder.AddListeningPort(server_address, server_credentials);
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    assert(!handlers_.empty());
    for (const auto& service : services_)
        builder.RegisterService(service.get());
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
    cq_ = builder.AddCompletionQueue();
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    // Proceed to the server's main loop.
    HandleRpcs(should_exit);
}

void
GRPCAsyncServer::CallData::Proceed()
{
    if (status_ == CREATE) {
        async_service_handler_->ListenForRequest(&ctx_, cq_, this);
        status_ = START_PROCESSING;
    } 
    else if (status_ == START_PROCESSING || status_ == CONTINUE_PROCESSING) {
        // Make new CallData instance to field any new requests that arrive
        if(status_ == START_PROCESSING)
            new CallData(async_service_handler_->CreateNewHandler(), cq_);
        status_ = PUSH_TO_BACK;
        const auto handler_status = async_service_handler_->SendResponse(this);
        if (handler_status == AsyncServiceHandler::FINISHED)
            status_ = FINISH; 
        else if (handler_status == AsyncServiceHandler::IDLE) {
           alarm_.Set(cq_, gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
               gpr_time_from_micros(100, GPR_TIMESPAN)), this); 
           status_ = CONTINUE_PROCESSING;
        }
    }
    else if (status_ == PUSH_TO_BACK) {
        status_ = CONTINUE_PROCESSING;
        alarm_.Set(cq_, gpr_now(GPR_CLOCK_REALTIME), this);
    }
    else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
    }
}

void
GRPCAsyncServer::HandleRpcs(const std::atomic_bool& should_exit) 
{
    assert(!handlers_.empty());
    // Spawn a new CallData instance to serve new clients.
    for (const auto& async_service_handler : handlers_)
        new CallData(async_service_handler, cq_.get());

    while (!should_exit) {
        void* tag = nullptr;  // uniquely identifies a request.
        bool ok = false;
        gpr_timespec deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
           gpr_time_from_micros(100, GPR_TIMESPAN));
        CompletionQueue::NextStatus stat = cq_->AsyncNext(&tag, &ok, deadline);
        if (stat == CompletionQueue::GOT_EVENT && ok) {
            static_cast<CallData*>(tag)->Proceed();
        }
    }
}

} // end namespace grpcserver

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

#pragma once
#include <memory>
#include <functional>
#include <assert.h>
#include "GRPCAsyncServer.h"

namespace grpcserver {

// This class implements all of the generic methods that the
// grpc async framework requires from a request handler,
// except for the SendResponse method which must be tailored to
// each request.
template <class ProtoGeneratedRequest, class ProtoGeneratedReply,
    template <class> class AsyncWriter, class Derived>
class GenericAsyncHandler : public grpcserver::AsyncServiceHandler
{
public:

    using GRPCWriter = AsyncWriter<ProtoGeneratedReply>;

    GenericAsyncHandler(std::function<void(::grpc::ServerContext*,
        ProtoGeneratedRequest* request, GRPCWriter* response,
        ::grpc::CompletionQueue*, ::grpc::ServerCompletionQueue*,
         void*)> request_service_func) :
        grpcserver::AsyncServiceHandler(),
        request_service_func_(request_service_func),
        responder_(nullptr) {}

    void ListenForRequest(grpc::ServerContext* ctx,
        grpc::ServerCompletionQueue* cq, void* opaque_handle) override
    {
        assert(!responder_.get());
        responder_.reset(new GRPCWriter(ctx));
        request_service_func_(ctx, &request_, responder_.get(),
           cq, cq, opaque_handle);
    }

    std::shared_ptr<AsyncServiceHandler> CreateNewHandler() const override
    {
        return std::shared_ptr<AsyncServiceHandler>(new Derived(request_service_func_));
    }

protected:
    std::function<void(::grpc::ServerContext*,
        ProtoGeneratedRequest* request, GRPCWriter* response,
        ::grpc::CompletionQueue*, ::grpc::ServerCompletionQueue*,
        void*)> request_service_func_;
    ProtoGeneratedRequest request_;
    std::unique_ptr<GRPCWriter> responder_;
};

} // end ns grpcserver

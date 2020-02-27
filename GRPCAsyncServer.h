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
#include <string>
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include <grpc++/impl/service_type.h>
#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <atomic>
#include <utility>

namespace grpcserver {

/*
 * Class AsyncServiceHandler is the base class abstraction for asynchronous
 * request/response handlers. Every async req/resp handler inherits from this class.
 *
 */
class AsyncServiceHandler {
public:
    AsyncServiceHandler() {}

    /*
     * Listen for an incoming request
     *
     * @param ctx Context for the rpc.
     * @param cq Completion queue that notifies us when a new event arrives
     * @param tag  opaque pointer to framework handler
     */
    virtual void ListenForRequest(grpc::ServerContext* ctx,
        grpc::ServerCompletionQueue* cq, void* tag) = 0;

    // Request has arrived; respond to it
    enum HandlerStatus {
        FINISHED,
        IDLE,
        ACTIVE  
    };
    virtual HandlerStatus SendResponse(void*) = 0;

    // The concrete class overrides this to create new handlers to field new requests, which
    // is necessary for asynchronicity. When a request is received, a new listener must
    // be launched to accept further requests; the incoming request is then handled. When
    // a response is complete, the handler is destroyed.
    virtual std::shared_ptr<AsyncServiceHandler> CreateNewHandler() const = 0;

    virtual ~AsyncServiceHandler() {}
};

// write for single request, streaming response
template <class Reply>
using AsyncStreamingWriter = grpc::ServerAsyncWriter<Reply>;

// Writer for request/response one-shots
template <class Reply>
using AsyncStandardWriter = grpc::ServerAsyncResponseWriter<Reply>;

/*
 * Class GRPCAsyncServer is the implementation of a GRPC asynchronous
 * server. It establishes a framework so that it can accept a vector of
 * request/response handlers. Each handler is represented by the abstract base class
 * AsyncServiceHandler. 
 */
class GRPCAsyncServer {
public:
    ~GRPCAsyncServer();

    // no copies, but moves are allowed
    GRPCAsyncServer(const GRPCAsyncServer&) = delete;
    GRPCAsyncServer& operator=(const GRPCAsyncServer&) = delete;
    GRPCAsyncServer(GRPCAsyncServer&&) = default;
    GRPCAsyncServer& operator=(GRPCAsyncServer&&) = default;

    /* GRPCAsyncServer constructor
     * @param port - port on which to listen
     * @param service- grpc service providing grpc api
     * @param handlers- service handlers that handle queries
     */
    GRPCAsyncServer(uint16_t port,
        const std::vector<std::shared_ptr<grpc::Service>>& services,
        const std::vector<std::shared_ptr<AsyncServiceHandler>>& handlers):
        cq_(nullptr),
        services_(services),
        handlers_(handlers),
        server_(nullptr),
        port_(port) {}

    void Run(const std::atomic_bool& should_exit);

    void Shutdown();

private:
    // This can be run in multiple threads if needed.
    void HandleRpcs(const std::atomic_bool& should_exit);

    // Class encompasing the state and logic needed to serve a request.
    class CallData {
    public:
        /* Start listening for a new request.
         * @param handler Handler to process the request/response
         * @param cq Completion queue that notifies us when a new event arrives
        */
        CallData(const std::shared_ptr<AsyncServiceHandler>& handler,
            grpc::ServerCompletionQueue* cq)
            : async_service_handler_(handler), cq_(cq), status_(CREATE), alarm_() {
            // Invoke the serving logic right away.
            Proceed();
        }
        void Proceed();
    private:
        // The means of communication with the gRPC runtime for an asynchronous
        // server.
        std::shared_ptr<AsyncServiceHandler> async_service_handler_;
        // The completion queue where we are notified that a we have a new event
        grpc::ServerCompletionQueue* cq_;
        // Context for the rpc, allowing to tweak aspects of it such as the use
        // of compression, authentication, as well as to send metadata back to the
        // client.
        grpc::ServerContext ctx_;
        // A small state machine that allows us to progress from starting a session to
        // sending responses to ending a session.
        enum CallStatus { CREATE, START_PROCESSING, CONTINUE_PROCESSING, PUSH_TO_BACK, FINISH };
        CallStatus status_;  // The current serving state.
        grpc::Alarm alarm_;
    };

private:
    std::unique_ptr<grpc::ServerCompletionQueue> cq_;
    std::vector<std::shared_ptr<grpc::Service>> services_;
    std::vector<std::shared_ptr<AsyncServiceHandler>> handlers_;
    std::unique_ptr<grpc::Server> server_;
    uint16_t port_;
};

} // end ns grpc

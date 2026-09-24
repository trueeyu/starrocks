// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/ev_http_server.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/ev_http_server.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>
#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/debug_util.h"
#include "util/errno.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"

namespace starrocks {

static void on_chunked(struct evhttp_request* ev_req, void* param) {
    auto* request = (HttpRequest*)ev_req->on_free_cb_arg;
    request->handler()->on_chunk_data(request);
}

static void on_free(struct evhttp_request* ev_req, void* arg) {
    auto* request = (HttpRequest*)arg;
    delete request;
}

static void on_request(struct evhttp_request* ev_req, void* arg) {
    auto request = (HttpRequest*)ev_req->on_free_cb_arg;
    if (request == nullptr) {
        // In this case, request's on_header return -1
        return;
    }
    request->handler()->handle(request);
}

static int on_header(struct evhttp_request* ev_req, void* param) {
    auto* server = (EvHttpServer*)ev_req->on_complete_cb_arg;
    return server->on_header(ev_req);
}

// param is pointer of EvHttpServer
static int on_connection(struct evhttp_request* req, void* param) {
    evhttp_request_set_header_cb(req, on_header);
    // only used on_complete_cb's argument
    evhttp_request_set_on_complete_cb(req, nullptr, param);
    return 0;
}

EvHttpServer::EvHttpServer(int port, int num_workers) : _port(port), _num_workers(num_workers), _real_port(0) {
    _host = BackendOptions::get_service_bind_address();
    DCHECK_GT(_num_workers, 0);
    auto res = pthread_rwlock_init(&_rw_lock, nullptr);
    DCHECK_EQ(res, 0);
}

EvHttpServer::EvHttpServer(std::string host, int port, int num_workers)
        : _host(std::move(host)), _port(port), _num_workers(num_workers), _real_port(0) {
    DCHECK_GT(_num_workers, 0);
    auto res = pthread_rwlock_init(&_rw_lock, nullptr);
    DCHECK_EQ(res, 0);
}

EvHttpServer::~EvHttpServer() {
    pthread_rwlock_destroy(&_rw_lock);
}

Status EvHttpServer::start() {
    // bind to
    RETURN_IF_ERROR(_bind());
    for (int i = 0; i < _num_workers; ++i) {
        auto worker = [this]() {
            struct event_base* base = event_base_new();
            if (base == nullptr) {
                LOG(WARNING) << "Couldn't create an event_base.";
                return;
            }
            pthread_rwlock_wrlock(&_rw_lock);
            _event_bases.push_back(base);
            pthread_rwlock_unlock(&_rw_lock);

            /* Create a new evhttp object to handle requests. */
            struct evhttp* http = evhttp_new(base);
            if (http == nullptr) {
                LOG(WARNING) << "Couldn't create an evhttp.";
                return;
            }

            pthread_rwlock_wrlock(&_rw_lock);
            _https.push_back(http);
            pthread_rwlock_unlock(&_rw_lock);

            auto res = evhttp_accept_socket(http, _server_fd);
            if (res < 0) {
                LOG(WARNING) << "evhttp accept socket failed"
                             << ", error:" << errno_to_string(errno);
                return;
            }

            evhttp_set_newreqcb(http, on_connection, this);
            evhttp_set_gencb(http, on_request, this);

            event_base_dispatch(base);
        };
        _workers.emplace_back(worker);
        Thread::set_thread_name(_workers.back(), "http_server");
    }
    return Status::OK();
}

void EvHttpServer::stop() {
    // break the base to stop the event
    for (auto base : _event_bases) {
        event_base_loopbreak(base);
    }

    // shutdown the socket to wake up the epoll_wait
    shutdown(_server_fd, SHUT_RDWR);
}

void EvHttpServer::join() {
    for (auto& thread : _workers) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    int old_fd = _server_fd;
    // close the socket at last
    close(_server_fd);

    // ---------------- DEBUG ONLY ----------------
    std::vector<int> debug_filler_fds;
    std::string debug_reused_by;
    if (config::debug_http_join_wait_fd_reuse_ms > 0) {
        // socket()/open() always return the lowest free fd. Occupy every free fd lower than old_fd, so that
        // the next socket() in the process (e.g. a brpc health check) lands on old_fd.
        while (true) {
            int fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
            if (fd < 0) {
                PLOG(WARNING) << "[DEBUG] failed to open filler fd";
                break;
            }
            if (fd < old_fd) {
                debug_filler_fds.push_back(fd);
                continue;
            }
            // No free fd below old_fd any more, give this one back.
            ::close(fd);
            break;
        }
        LOG(WARNING) << "[DEBUG] http server closed fd " << old_fd << ", occupied " << debug_filler_fds.size()
                     << " lower free fds, waiting for reuse";

        MonotonicStopWatch watch;
        watch.start();
        const uint64_t timeout_ns = static_cast<uint64_t>(config::debug_http_join_wait_fd_reuse_ms) * 1000000UL;
        // Only stop waiting once old_fd is reused by a socket (e.g. a brpc health check). A number taken by a
        // regular file (e.g. a periodic /proc reader) is released again soon, so keep waiting.
        char target[256] = {0};
        std::string link = "/proc/self/fd/" + std::to_string(old_fd);
        ssize_t n = -1;
        while (watch.elapsed_time() < timeout_ns) {
            memset(target, 0, sizeof(target));
            n = ::readlink(link.c_str(), target, sizeof(target) - 1);
            if (n > 0 && strncmp(target, "socket:", 7) == 0) {
                break;
            }
            usleep(1000);
        }
        if (n > 0) {
            debug_reused_by = target;
        }
        LOG(WARNING) << "[DEBUG] http server fd " << old_fd << " reused by: " << (n > 0 ? target : "<none>")
                     << ", waited " << watch.elapsed_time() / 1000 << "us, evhttp_free will close it now";
    }
    // --------------------------------------------

    // free the evhttp and event_base
    for (auto http : _https) {
        evhttp_free(http);
    }

    // DEBUG ONLY: check whether evhttp_free() closed the fd that another thread had reused.
    if (!debug_reused_by.empty()) {
        char target[256] = {0};
        std::string link = "/proc/self/fd/" + std::to_string(old_fd);
        ssize_t n = ::readlink(link.c_str(), target, sizeof(target) - 1);
        const std::string after = n > 0 ? target : "<closed>";
        LOG(WARNING) << "[DEBUG] after evhttp_free: fd " << old_fd << " " << debug_reused_by << " -> " << after
                     << (after == debug_reused_by ? " (still open)" : " (CLOSED BY evhttp_free)");
    }
    for (int fd : debug_filler_fds) {
        ::close(fd);
    }

    for (auto base : _event_bases) {
        event_base_free(base);
    }
}

Status EvHttpServer::_bind() {
    butil::EndPoint point;
    auto res = butil::str2endpoint(_host.c_str(), _port, &point);
    if (res < 0) {
        std::stringstream ss;
        ss << "convert address failed, host=" << _host << ", port=" << _port;
        return Status::InternalError(ss.str());
    }
    // reuse_addr arg is removed in brpc 0.9.7 and use gflag instead.
    // default reuse_addr is true and reuse_port is false.
    _server_fd = butil::tcp_listen(point);
    if (_server_fd < 0) {
        std::stringstream ss;
        ss << "Failed to listen port. port: " << _port << ", error: " << errno_to_string(errno);
        return Status::InternalError(ss.str());
    }
    if (_port == 0) {
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        const int rc = getsockname(_server_fd, (struct sockaddr*)&addr, &socklen);
        if (rc == 0) {
            _real_port = ntohs(addr.sin_port);
        }
    }
    res = butil::make_non_blocking(_server_fd);
    if (res < 0) {
        std::stringstream ss;
        ss << "Failed to generate a non-blocking socket. error: " << errno_to_string(errno);
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

bool EvHttpServer::register_handler(const HttpMethod& method, const std::string& path, HttpHandler* handler) {
    if (handler == nullptr) {
        LOG(WARNING) << "dummy handler for http method " << method << " with path " << path;
        return false;
    }

    bool result = true;
    pthread_rwlock_wrlock(&_rw_lock);
    PathTrie<HttpHandler*>* root = nullptr;
    switch (method) {
    case GET:
        root = &_get_handlers;
        break;
    case PUT:
        root = &_put_handlers;
        break;
    case POST:
        root = &_post_handlers;
        break;
    case DELETE:
        root = &_delete_handlers;
        break;
    case HEAD:
        root = &_head_handlers;
        break;
    case OPTIONS:
        root = &_options_handlers;
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << method;
        result = false;
    }
    if (result) {
        result = root->insert(path, handler);
    }
    pthread_rwlock_unlock(&_rw_lock);

    return result;
}

void EvHttpServer::register_static_file_handler(HttpHandler* handler) {
    DCHECK(handler != nullptr);
    DCHECK(_static_file_handler == nullptr);
    pthread_rwlock_wrlock(&_rw_lock);
    _static_file_handler = handler;
    pthread_rwlock_unlock(&_rw_lock);
}

int EvHttpServer::on_header(struct evhttp_request* ev_req) {
    std::unique_ptr<HttpRequest> request(new HttpRequest(ev_req));
    auto res = request->init_from_evhttp();
    if (res < 0) {
        return -1;
    }
    auto handler = _find_handler(request.get());
    if (handler == nullptr) {
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        HttpChannel::send_reply(request.get(), HttpStatus::NOT_FOUND, "Not Found");
        return 0;
    }
    // set handler before call on_header, because handler_ctx will set in on_header
    request->set_handler(handler);
    res = handler->on_header(request.get());
    if (res < 0) {
        // reply has already sent by handler's on_header
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        return 0;
    }

    // If request body would be big(greater than 1GB),
    // it is better that request_will_be_read_progressively is set true,
    // this can make body read in chunk, not in total
    if (handler->request_will_be_read_progressively()) {
        evhttp_request_set_chunked_cb(ev_req, on_chunked);
    }

    evhttp_request_set_on_free_cb(ev_req, on_free, request.release());
    return 0;
}

HttpHandler* EvHttpServer::_find_handler(HttpRequest* req) {
    auto& path = req->raw_path();

    HttpHandler* handler = nullptr;

    pthread_rwlock_rdlock(&_rw_lock);
    switch (req->method()) {
    case GET:
        _get_handlers.retrieve(path, &handler, req->params());
        // Static file handler is a fallback handler
        if (handler == nullptr) {
            handler = _static_file_handler;
        }
        break;
    case PUT:
        _put_handlers.retrieve(path, &handler, req->params());
        break;
    case POST:
        _post_handlers.retrieve(path, &handler, req->params());
        break;
    case DELETE:
        _delete_handlers.retrieve(path, &handler, req->params());
        break;
    case HEAD:
        _head_handlers.retrieve(path, &handler, req->params());
        break;
    case OPTIONS:
        _options_handlers.retrieve(path, &handler, req->params());
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << req->method();
        break;
    }
    pthread_rwlock_unlock(&_rw_lock);
    return handler;
}

} // namespace starrocks

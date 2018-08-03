#include "cocaine/vicodyn/proxy.hpp"

#include "cocaine/api/vicodyn/balancer.hpp"
#include "cocaine/format/ptr.hpp"
#include "cocaine/format/peer.hpp"
#include "cocaine/repository/vicodyn/balancer.hpp"

#include "cocaine/vicodyn/peer.hpp"
#include "cocaine/vicodyn/request_context.hpp"
#include "cocaine/service/node/slave/error.hpp"

#include <cocaine/context.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>
#include <cocaine/format/exception.hpp>
#include <cocaine/logging.hpp>

#include <blackhole/logger.hpp>
#include <blackhole/wrapper.hpp>
#include <cocaine/rpc/slot.hpp>

#include <cocaine/traits/map.hpp>
#include <cocaine/vicodyn/error.hpp>

namespace cocaine {
namespace vicodyn {

using app_tag = io::stream_of<std::string>::tag;

template<class Tag>
class discardable : public dispatch<Tag> {
public:
    using discarder_t = std::function<void(const std::error_code&)>;

    discardable(const std::string& name):
        dispatch<app_tag>(name)
    {}

    auto discard(const std::error_code& ec) -> void override {
        if(discarder != nullptr) {
            discarder(ec);
        }
    }

    auto on_discard(discarder_t d) -> void {
        discarder = std::move(d);
    }

private:
    discarder_t discarder;
};

class safe_stream_t {
    boost::optional<upstream<app_tag>> stream_;
    std::atomic_bool closed_{false};

public:
    using protocol = io::protocol<app_tag>::scope;

    safe_stream_t() = default;

    safe_stream_t(upstream<app_tag>&& stream)
        : stream_(std::move(stream))
    {}

    auto operator=(safe_stream_t&& that) -> safe_stream_t& {
        stream_ = std::move(that.stream_);
        closed_ = that.closed_.load();
        return *this;
    }

    auto chunk(const hpack::headers_t& headers, std::string data) -> bool {
        if(!closed_ && stream_) {
            stream_ = stream_->send<protocol::chunk>(headers, std::move(data));
            return true;
        }
        return false;
    }

    auto close(const hpack::headers_t& headers) -> bool {
        if(!closed_.exchange(true) && stream_) {
            stream_->send<protocol::choke>(headers);
            return true;
        }
        return false;
    }

    auto error(const hpack::headers_t& headers, std::error_code ec, std::string msg) -> bool {
        if(!closed_.exchange(true) && stream_) {
            stream_->send<protocol::error>(headers, ec, std::move(msg));
            return true;
        }
        return false;
    }
};

class client_stream_t {
    safe_stream_t stream_;
    std::atomic_bool started_{false};

    logging::logger_t& logger_;

private:
    template<class F>
    auto catch_connection_error(F&& f) -> bool {
        try {
            return f();
        } catch (const std::system_error& e) {
            COCAINE_LOG_WARNING(logger_, "connection error with client - exit: {}", error::to_string(e));
        }
        return false;
    }

public:
    client_stream_t(upstream<app_tag>&& stream, logging::logger_t& logger)
        : stream_(std::move(stream))
        , logger_(logger) {
    }

    auto started() const -> bool {
        return started_;
    }

    auto chunk(const hpack::headers_t& headers, std::string data) -> bool {
        started_ = true;
        return catch_connection_error([&]() {
            return stream_.chunk(headers, std::move(data));
        });
    }

    auto close(const hpack::headers_t& headers) -> bool {
        started_ = true;
        return catch_connection_error([&]() {
            return stream_.close(headers);
        });
    }

    auto error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> bool {
        COCAINE_LOG_WARNING(logger_, "sending error to client {}({}) - {}", ec.message(), ec.value(), msg);
        started_ = true;
        return catch_connection_error([&]() {
            return stream_.error(headers, ec, msg);
        });
    }
};

class endpoint_t {
    using clock_t = peers_t::clock_t;

    std::shared_ptr<peer_t> peer_;
    safe_stream_t stream_;
    std::unique_ptr<logging::logger_t> logger_;
    clock_t::time_point start_time_;
    bool failed_ = false;

private:
    template<class F>
    auto catch_connection_error(F&& f) -> bool {
        try {
            f();
            return true;
        } catch (const std::system_error& e) {
            COCAINE_LOG_WARNING(logger_, "connection error with peer - schedule reconnect: {}", error::to_string(e));
            peer_->schedule_reconnect();
        }
        return false;
    }

public:
    endpoint_t(const api::vicodyn::balancer_ptr& balancer, const hpack::headers_t& headers, const std::string& event,
                    const std::shared_ptr<request_context_t>& request_context, logging::logger_t& logger)
        : peer_(balancer->choose_peer(request_context, headers, event))
        , logger_(std::make_unique<blackhole::wrapper_t>(logger, blackhole::attributes_t{{"peer", peer_->uuid()}}))
        , start_time_(clock_t::now()) {
        COCAINE_LOG_DEBUG(logger_, "peer was selected");
        request_context->mark_used_peer(peer_);
    }

    auto open_stream(std::string app_name, hpack::headers_t headers, std::string event,
                    std::shared_ptr<dispatch<app_tag>> backward) -> bool {
        COCAINE_LOG_DEBUG(logger_, "open stream to peer");
        return catch_connection_error([&]() {
            auto upstream = peer_->open_stream<io::node::enqueue>(std::move(backward), std::move(headers),
                            std::move(app_name), std::move(event));
            stream_ = safe_stream_t(std::move(upstream));
        });
    }

    auto chunk(const hpack::headers_t& headers, std::string data) -> bool {
        COCAINE_LOG_DEBUG(logger_, "send chunk to peer");
        return catch_connection_error([&]() {
            stream_.chunk(headers, std::move(data));
        });
    }

    auto close(const hpack::headers_t& headers) -> bool {
        COCAINE_LOG_DEBUG(logger_, "send chunk to peer");
        return catch_connection_error([&]() {
            stream_.close(headers);
        });
    }

    auto error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> bool {
        COCAINE_LOG_INFO(logger_, "send error to peer {}({}) - {}");
        failed_ = true;
        return catch_connection_error([&]() {
            stream_.error(headers, ec, msg);
        });
    }

    auto failed() const -> bool {
        return failed_;
    }

    auto elapsed_time() const -> clock_t::duration {
        return clock_t::now() - start_time_;
    }

    auto peer() const -> const peer_t& {
        return *peer_;
    }
};

class buffer_t {
    std::string event_;
    hpack::headers_t headers_;

    std::vector<std::pair<hpack::headers_t, std::string>> chunks_;
    boost::optional<hpack::headers_t> choke_;
    bool overfull_ = false;

    std::size_t capacity_;


private:
    auto try_account(const hpack::headers_t& headers, std::size_t data_size = 0) -> bool {
        if (overfull()) {
            return false;
        }
        for (const auto& header : headers) {
            data_size += header.http2_size();
        }
        if (data_size > capacity_) {
            // If we exceed the buffer once, we'll disable it for the request forever, because it has became invalid.
            overfull_ = true;
            clear();
            return false;
        }
        capacity_ -= data_size;
        return true;
    }


public:
    buffer_t(std::size_t capacity)
        : capacity_(capacity) {
    }

    auto overfull() const -> bool {
        return overfull_;
    }

    auto store_event(const hpack::headers_t& headers, const std::string& event) -> void {
        if (try_account(headers, event.size())) {
            headers_ = headers;
            event_ = event;
        }
    }

    auto event() const -> const std::string& {
        return event_;
    }

    auto headers() const -> const hpack::headers_t& {
        return headers_;
    }

    auto chunk(const hpack::headers_t& headers, const std::string& data) -> void {
        if (try_account(headers, data.size())) {
            chunks_.emplace_back(headers, data);
        }
    }

    auto close(const hpack::headers_t& headers) -> void {
        if (try_account(headers)) {
            choke_ = headers;
        }
    }

    auto flush(endpoint_t& stream) const -> void {
        for(const auto& chunk : chunks_) {
            stream.chunk(chunk.first, chunk.second);
        }
        if (choke_) {
            stream.close(choke_.get());
        }
    }

    auto clear() -> void {
        event_.clear();
        event_.shrink_to_fit();
        headers_.clear();
        headers_.shrink_to_fit();
        chunks_.clear();
        chunks_.shrink_to_fit();
        choke_.reset();
    }
};

class vicodyn_dispatch_t : public std::enable_shared_from_this<vicodyn_dispatch_t> {
    using protocol = io::protocol<app_tag>::scope;

    struct data_t {
        std::unique_ptr<endpoint_t> endpoint;
        buffer_t buffer;
    } unsafe_;

    proxy_t& proxy;

    std::shared_ptr<request_context_t> request_context;

    discardable<app_tag> forward_dispatch;
    discardable<app_tag> backward_dispatch;

    client_stream_t client_stream;

    boost::shared_mutex mutex;


public:
    vicodyn_dispatch_t(proxy_t& _proxy, const std::string& name, upstream<app_tag> c_stream)
        : unsafe_{nullptr, _proxy.buffer_capacity}
        , proxy(_proxy)
        , request_context(std::make_shared<request_context_t>(*_proxy.logger))
        , forward_dispatch(name + "/forward")
        , backward_dispatch(name + "/backward")
        , client_stream(std::move(c_stream), *_proxy.logger)
    {
        namespace ph = std::placeholders;

        forward_dispatch.on<protocol::chunk>()
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_chunk, this, ph::_1, ph::_2));

        forward_dispatch.on<protocol::choke>()
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_choke, this, ph::_1));

        forward_dispatch.on<protocol::error>()
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_error, this, ph::_1, ph::_2, ph::_3));

        forward_dispatch.on_discard([&](const std::error_code& ec) {
            apply([&](data_t& data) {
                data.endpoint->error({}, ec, "client has been disconnected");
            });
        });

        backward_dispatch.on<protocol::chunk>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_chunk, this, ph::_1, ph::_2));

        backward_dispatch.on<protocol::choke>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_choke, this, ph::_1));

        backward_dispatch.on<protocol::error>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_error, this, ph::_1, ph::_2, ph::_3));

        backward_dispatch.on_discard([&](const std::error_code& ec) {
            client_stream.error({}, ec, "peer has been disconnected");
        });
    }

    auto enqueue(const hpack::headers_t& headers, const std::string& event) -> std::shared_ptr<dispatch<app_tag>> {
        unsafe_.buffer.store_event(headers, event);
        if (!choose_endpoint(headers, event)) {
            retry();
        }
        return shared_forward_dispatch();
    }

private:
    template<class F>
    auto apply_shared(F&& f) -> decltype(f(std::declval<const data_t&>())) {
        boost::shared_lock<boost::shared_mutex> lock(mutex);
        return f(unsafe_);
    }

    template<class F>
    auto apply(F&& f) -> decltype(f(std::declval<data_t&>())) {
        boost::unique_lock<boost::shared_mutex> lock(mutex);
        return f(unsafe_);
    }

    auto on_forward_chunk(const hpack::headers_t& headers, std::string chunk) -> void {
        const auto success = apply([&](data_t& safe) {
            // For this place and below: we don't need to store and use cache if we have started
            // reply to backward stream
            if (!client_stream.started()) {
                safe.buffer.chunk(headers, chunk);
            }
            return safe.endpoint->chunk(headers, std::move(chunk));
        });
        if (!success) {
            retry_or_fail();
        }
        request_context->add_checkpoint("after_fchunk");
    }

    auto on_forward_choke(const hpack::headers_t& headers) -> void {
        const auto success = apply([&](data_t& safe) {
            if (!client_stream.started()) {
                safe.buffer.close(headers);
            }
            return safe.endpoint->close(headers);
        });
        if (!success) {
            retry_or_fail();
        }
        request_context->add_checkpoint("after_fchoke");
    }

    auto on_forward_error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> void {
        const auto success = apply([&](data_t& safe) {
            return safe.endpoint->error(headers, ec, msg);
        });
        if (!success) {
            // This retry unconditionally fails, because we don't retry failed requests
            retry_or_fail();
        }
        request_context->add_checkpoint("after_ferror");
    }

    auto on_backward_chunk(const hpack::headers_t& headers, std::string chunk) -> void {
        if (!client_stream.chunk(headers, std::move(chunk))) {
            apply([&](data_t& safe) {
                //safe.endpoint->error(headers, ec, msg);
            });
        }
        request_context->add_checkpoint("after_bchunk");
    }

    auto on_backward_choke(const hpack::headers_t& headers) -> void {
        if (client_stream.close(headers)) {
            auto request_duration = apply_shared([&](const data_t& safe) {
                return std::make_pair(safe.endpoint->peer().uuid(), safe.endpoint->elapsed_time());
            });
            proxy.peers.add_app_request_duration(request_duration.first, proxy.app_name, request_duration.second);
        } else {
            apply([&](data_t& safe) {
                //safe.endpoint->error(headers, ec, msg);
            });
        }
        request_context->add_checkpoint("after_bchoke");
        request_context->finish();
    }

    auto on_backward_error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> void {
        apply_shared([&](const data_t& safe) {
            proxy.balancer->on_error(safe.endpoint->peer(), ec, msg);
        });
        if (proxy.balancer->is_recoverable(ec)) {
            retry_or_fail();
        } else {
            if (!client_stream.error(headers, ec, msg)) {
                apply([&](data_t& safe) {
                    //safe.endpoint->error(headers, ec, msg);
                });
            }
            request_context->add_checkpoint("after_berror");
            request_context->fail(ec, msg);
        }
    };

    auto shared_backward_dispatch() -> std::shared_ptr<dispatch<app_tag>> {
        return std::shared_ptr<dispatch<app_tag>>(shared_from_this(), &backward_dispatch);
    }

    auto shared_forward_dispatch() -> std::shared_ptr<dispatch<app_tag>> {
        return std::shared_ptr<dispatch<app_tag>>(shared_from_this(), &forward_dispatch);
    }

    auto choose_endpoint(const hpack::headers_t& headers, const std::string& event) -> bool {
        // Used under lock higher in the stack or in the enqueue
        unsafe_.endpoint = std::make_unique<endpoint_t>(proxy.balancer, headers, event, request_context, *proxy.logger);
        return unsafe_.endpoint->open_stream(proxy.app_name, headers, event, shared_backward_dispatch());
    }

    auto retry() -> void {
        COCAINE_LOG_INFO(proxy.logger, "retrying");
        if (client_stream.started()) {
            throw error_t("retry is forbidden - response chunk was sent");
        }
        apply([&](data_t& safe) {
            if (safe.endpoint && safe.endpoint->failed()) {
                throw error_t("retry is forbidden - client sent error");
            }
            if (safe.buffer.overfull()) {
                throw error_t("retry is forbidden - buffer overflow");
            }
            do {
                request_context->register_retry();
                if (request_context->retry_count() >= proxy.balancer->retry_count()) {
                    throw error_t("retry is forbidden - maximum number of tries reached");
                }
                request_context->add_checkpoint("retry");
            } while (!choose_endpoint(safe.buffer.headers(), safe.buffer.event()));
            safe.buffer.flush(*safe.endpoint);
        });
        request_context->add_checkpoint("after_retry");
    }

    auto retry_or_fail() -> void {
        try {
            retry();
        } catch (const std::system_error& e) {
            client_stream.error({}, e.code(), e.what());
        }
    }
};

auto proxy_t::make_balancer(const dynamic_t& args, const dynamic_t::object_t& extra) -> api::vicodyn::balancer_ptr {
    auto balancer_conf = args.as_object().at("balancer", dynamic_t::empty_object);
    auto name = balancer_conf.as_object().at("type", "simple").as_string();
    auto balancer_args = balancer_conf.as_object().at("args", dynamic_t::empty_object).as_object();
    return context.repository().get<api::vicodyn::balancer_t>(name, context, peers, loop, app_name,
                                                              balancer_args, extra);
}

proxy_t::proxy_t(context_t& context, asio::io_service& loop, peers_t& peers, const std::string& name,
                const dynamic_t& args, const dynamic_t::object_t& extra) :
    dispatch(name),
    context(context),
    loop(loop),
    peers(peers),
    app_name(name.substr(sizeof("virtual::") - 1)),
    balancer(make_balancer(args, extra)),
    buffer_capacity(args.as_object().at("buffer_capacity_kb", 10240U).as_uint()),
    logger(context.log(name))
{
    COCAINE_LOG_DEBUG(logger, "created proxy for app {}", app_name);
    on<event_t>([&](const hpack::headers_t& headers, slot_t::tuple_type&& args,
                    slot_t::upstream_type&& client_stream) {
        auto event = std::get<0>(args);
        auto dispatch_name = format("{}/{}/streaming", this->name(), event);
        auto dispatch = std::make_shared<vicodyn_dispatch_t>(*this, dispatch_name, client_stream);
        return result_t(dispatch->enqueue(headers, event));
    });
}

auto proxy_t::empty() -> bool {
    return peers.apply_shared([&](const peers_t::data_t& data) -> bool {
        auto it = data.apps.find(app_name);
        if(it == data.apps.end() || it->second.empty()) {
            return true;
        }
        return false;
    });
}

auto proxy_t::size() -> size_t {
    return peers.apply_shared([&](const peers_t::data_t& data) -> size_t {
        auto it = data.apps.find(app_name);
        if(it == data.apps.end()) {
            return 0u;
        }
        return it->second.size();
    });
}

} // namespace vicodyn
} // namespace cocaine

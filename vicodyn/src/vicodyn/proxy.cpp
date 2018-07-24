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
    std::atomic_bool started_{false};
    std::atomic_bool closed_{false};

public:
    using protocol = io::protocol<app_tag>::scope;

    safe_stream_t() = default;

    safe_stream_t(upstream<app_tag>&& stream)
        : stream_(std::move(stream))
    {}

    operator bool() {
        return stream_.is_initialized();
    }

    auto operator=(safe_stream_t&& that) -> safe_stream_t& {
        stream_ = std::move(that.stream_);
        started_ = that.started_.load();
        closed_ = that.closed_.load();
        return *this;
    }

    auto started() const -> bool {
        return started_;
    }

    auto chunk(const hpack::headers_t& headers, std::string data) -> bool {
        if(!closed_ && stream_) {
            started_ = true;
            stream_ = stream_->send<protocol::chunk>(headers, std::move(data));
            return true;
        }
        return false;
    }

    auto close(const hpack::headers_t& headers) -> bool {
        if(!closed_.exchange(true) && stream_) {
            started_ = true;
            stream_->send<protocol::choke>(headers);
            return true;
        }
        return false;
    }

    auto error(const hpack::headers_t& headers, std::error_code ec, std::string msg) -> bool {
        if(!closed_.exchange(true) && stream_) {
            started_ = true;
            stream_->send<protocol::error>(headers, std::move(ec), std::move(msg));
            return true;
        }
        return false;
    }
};

class buffer_t {
    struct error_buffer_t {
        hpack::headers_t headers;
        std::error_code ec;
        std::string msg;
    };

    std::string event_;
    hpack::headers_t headers_;

    std::vector<std::string> chunks_;
    std::vector<hpack::headers_t> chunk_headers_;

    boost::optional<error_buffer_t> error_;
    boost::optional<hpack::headers_t> choke_;

    std::size_t size_ = 0;
    std::size_t max_size_;


private:
    auto account_headers(const hpack::headers_t& headers) -> bool {
        if (overfull()) {
            return false;
        }
        for (const auto& header : headers) {
            size_ += header.http2_size();
        }
        return !overfull();
    }

    auto account_data(const std::string& data) -> bool {
        if (overfull()) {
            return false;
        }
        size_ += data.size();
        return !overfull();
    }


public:
    buffer_t(std::size_t max_size)
        : max_size_(max_size) {}

    auto overfull() const -> bool {
        return size_ > max_size_;
    }

    auto set_event(const hpack::headers_t& headers, const std::string& event) -> void {
        if (account_headers(headers) && account_data(event)) {
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
        if (account_headers(headers) && account_data(data)) {
            chunk_headers_.push_back(headers);
            chunks_.push_back(data);
        }
    }

    auto close(const hpack::headers_t& headers) -> void {
        if (account_headers(headers)) {
            choke_ = headers;
        }
    }

    auto error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> void {
        if (account_headers(headers) && account_data(msg)) {
            error_ = {headers, ec, msg};
        }
    }

    auto flush(safe_stream_t& stream) const -> void {
        if (overfull()) {
            return;
        }
        for(size_t i = 0; i < chunks_.size(); i++) {
            stream.chunk(chunk_headers_[i], chunks_[i]);
        }
        if (choke_) {
            stream.close(choke_.get());
        } else if (error_) {
            stream.error(error_->headers, error_->ec, error_->msg);
        }
    }
};

class vicodyn_dispatch_t : public std::enable_shared_from_this<vicodyn_dispatch_t> {
    using protocol = io::protocol<app_tag>::scope;
    using clock_t = peers_t::clock_t;

    struct endpoint_t {
        std::shared_ptr<peer_t> peer;
        safe_stream_t forward_stream;
        std::unique_ptr<logging::logger_t> logger;
        clock_t::time_point start_time;
    };

    proxy_t& proxy;

    buffer_t buffer;
    std::shared_ptr<request_context_t> request_context;
    endpoint_t endpoint;

    discardable<app_tag> forward_dispatch;
    discardable<app_tag> backward_dispatch;

    safe_stream_t backward_stream;

    synchronized<void> mutex;

public:
    class check_stream_t {
        vicodyn_dispatch_t* parent;
    public:
        explicit
        check_stream_t(vicodyn_dispatch_t* parent) :
            parent(parent)
        {}

        template<typename Event, typename F, typename... Args>
        auto
        operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) -> void {
            if(!parent->endpoint.forward_stream) {
                COCAINE_LOG_WARNING(parent->endpoint.logger, "skipping sending {} - forward stream is missing"
                    "(this can happen if enqueue was unsuccessfull)", Event::alias());
                return;
            }
            return fn(headers, std::forward<Args>(args)...);
        }
    };

    class locked_t {
        vicodyn_dispatch_t* parent;
    public:
        explicit
        locked_t(vicodyn_dispatch_t* parent) :
            parent(parent)
        {}

        template<typename Event, typename F, typename... Args>
        auto
        operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) -> void {
            auto guard = parent->mutex.synchronize();
            return fn(headers, std::forward<Args>(args)...);
        }
    };

    class catcher_t {
        vicodyn_dispatch_t* parent;
    public:
        explicit
        catcher_t(vicodyn_dispatch_t* parent) :
            parent(parent)
        {}

        template<typename Event, typename F, typename... Args>
        auto
        operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) -> void {
            try {
                return fn(headers, std::forward<Args>(args)...);
            }
            catch(const std::system_error& e) {
                COCAINE_LOG_WARNING(parent->endpoint.logger, "failed to send error to forward dispatch - {}",
                                error::to_string(e));
                parent->backward_stream.error({}, make_error_code(vicodyn_errors::failed_to_send_error_to_forward),
                                "failed to send error to forward dispatch");
                parent->endpoint.peer->schedule_reconnect();
            }
        }
    };

    vicodyn_dispatch_t(proxy_t& _proxy, const std::string& name, upstream<app_tag> b_stream) :
        proxy(_proxy),
        buffer(proxy.max_buffer_size),
        request_context(std::make_shared<request_context_t>(*proxy.logger)),
        forward_dispatch(name + "/forward"),
        backward_dispatch(name + "/backward"),
        backward_stream(std::move(b_stream))
    {
        namespace ph = std::placeholders;

        check_stream_t check_stream(this);
        locked_t locked(this);
        catcher_t catcher(this);
        forward_dispatch.on<protocol::chunk>()
            .with_middleware(locked)
            .with_middleware(check_stream)
            .with_middleware(catcher)
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_chunk, this, ph::_1, ph::_2));

        forward_dispatch.on<protocol::choke>()
            .with_middleware(locked)
            .with_middleware(check_stream)
            .with_middleware(catcher)
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_choke, this, ph::_1));

        forward_dispatch.on<protocol::error>()
            .with_middleware(locked)
            .with_middleware(check_stream)
            .with_middleware(catcher)
            .execute(std::bind(&vicodyn_dispatch_t::on_forward_error, this, ph::_1, ph::_2, ph::_3));

        forward_dispatch.on_discard([&](const std::error_code&){
            on_client_disconnection();
        });


        backward_dispatch.on<protocol::chunk>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_chunk, this, ph::_1, ph::_2));

        backward_dispatch.on<protocol::choke>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_choke, this, ph::_1));

        backward_dispatch.on<protocol::error>()
            .execute(std::bind(&vicodyn_dispatch_t::on_backward_error, this, ph::_1, ph::_2, ph::_3));

        backward_dispatch.on_discard([&](const std::error_code& ec) {
            try {
                backward_stream.error({}, make_error_code(vicodyn_errors::upstream_disconnected),
                                      "vicodyn upstream has been disconnected");
            } catch (const std::exception& e) {
                COCAINE_LOG_WARNING(endpoint.logger, "could not send error {} to upstream - {}", ec, e);
            }
        });
    }

    ~vicodyn_dispatch_t() {
    }

    auto enqueue(const hpack::headers_t& headers, std::string event) -> std::shared_ptr<dispatch<app_tag>> {
        return mutex.apply([&](){
            return enqueue_unsafe(headers, std::move(event));
        });
    }

private:
    auto on_forward_chunk(const hpack::headers_t& headers, std::string chunk) -> void {
        COCAINE_LOG_DEBUG(endpoint.logger, "processing chunk");
        // For this place and below: we don't need to store and use cache if we have started
        // reply to backward stream
        if (!backward_stream.started()) {
            buffer.chunk(headers, chunk);
        }
        endpoint.forward_stream.chunk(headers, std::move(chunk));
        request_context->add_checkpoint("after_fchunk");
    }

    auto on_forward_choke(const hpack::headers_t& headers) -> void {
        COCAINE_LOG_DEBUG(endpoint.logger, "processing choke");
        if (!backward_stream.started()) {
            buffer.close(headers);
        }
        endpoint.forward_stream.close(headers);
        request_context->add_checkpoint("after_fchoke");
    }

    auto on_forward_error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> void {
        COCAINE_LOG_INFO(endpoint.logger, "processing error");
        if (!backward_stream.started()) {
            buffer.error(headers, ec, msg);
        }
        endpoint.forward_stream.error(headers, ec, msg);
        request_context->add_checkpoint("after_ferror");
    }

    auto on_backward_chunk(const hpack::headers_t& headers, std::string chunk) -> void {
        try {
            backward_stream.chunk(headers, std::move(chunk));
            request_context->add_checkpoint("after_bchunk");
        } catch (const std::system_error& e) {
            on_client_disconnection();
        }
    }

    auto on_backward_error(const hpack::headers_t& headers, const std::error_code& ec, const std::string& msg) -> void {
        COCAINE_LOG_WARNING(endpoint.logger, "received error from peer {}({}) - {}", ec.message(), ec.value(), msg);
        proxy.balancer->on_error(endpoint.peer, ec, msg);
        if(proxy.balancer->is_recoverable(endpoint.peer, ec)) {
            try {
                request_context->add_checkpoint("recoverable_error");
                retry();
            } catch(const std::system_error& e) {
                COCAINE_LOG_WARNING(endpoint.logger, "failed to retry enqueue - {}", e.what());
                backward_stream.error({}, make_error_code(vicodyn_errors::failed_to_retry_enqueue), e.what());
                request_context->add_checkpoint("after_recoverable_error_failed_retry");
            }
        } else {
            try {
                backward_stream.error(headers, ec, msg);
                request_context->add_checkpoint("after_berror");
            } catch (const std::system_error& e) {
                on_client_disconnection();
            }
            request_context->fail(ec, msg);
        }
    };


    auto on_backward_choke(const hpack::headers_t& headers) -> void {
        try {
            if(backward_stream.close(headers)) {
                proxy.peers.add_app_request_timing(endpoint.peer->uuid(), proxy.app_name, endpoint.start_time,
                                clock_t::now() - endpoint.start_time);
                request_context->add_checkpoint("after_bchoke");
            }
        } catch (const std::system_error& e) {
            on_client_disconnection();
        }
        request_context->finish();
    }

    auto retry() -> void {
        mutex.apply([&](){
            retry_unsafe();
        });
    }

    auto on_client_disconnection() -> void {
        // TODO: Do we need lock here?
        COCAINE_LOG_DEBUG(endpoint.logger, "sending discard frame");
        auto ec = make_error_code(error::dispatch_errors::not_connected);
        endpoint.forward_stream.error({}, ec, "vicodyn client was disconnected");
    }

    auto shared_backward_dispatch() -> std::shared_ptr<dispatch<app_tag>> {
        return std::shared_ptr<dispatch<app_tag>>(shared_from_this(), &backward_dispatch);
    }

    auto shared_forward_dispatch() -> std::shared_ptr<dispatch<app_tag>> {
        return std::shared_ptr<dispatch<app_tag>>(shared_from_this(), &forward_dispatch);
    }

    auto choose_endpoint(hpack::headers_t headers, std::string event) -> void {
        endpoint.peer = proxy.balancer->choose_peer(request_context, headers, event);
        request_context->mark_used_peer(endpoint.peer);
        endpoint.logger = std::make_unique<blackhole::wrapper_t>(*proxy.logger,
                        blackhole::attributes_t{{"peer", endpoint.peer->uuid()}});
        endpoint.start_time = clock_t::now();
        auto u = endpoint.peer->open_stream<io::node::enqueue>(shared_backward_dispatch(), std::move(headers),
                        proxy.app_name, std::move(event));
        endpoint.forward_stream = safe_stream_t(std::move(u));
    }

    auto enqueue_unsafe(hpack::headers_t headers, std::string event) -> std::shared_ptr<dispatch<app_tag>> {
        try {
            buffer.set_event(headers, event);

            choose_endpoint(std::move(headers), std::move(event));

            request_context->add_checkpoint("after_enqueue");
        } catch (const std::system_error& e) {
            COCAINE_LOG_WARNING(endpoint.logger, "failed to send enqueue to forward stream - {}", error::to_string(e));
            endpoint.peer->schedule_reconnect();
            //TODO: maybe cycle here?
            try {
                retry_unsafe();
            } catch(std::system_error& e) {
                COCAINE_LOG_WARNING(endpoint.logger, "could not retry enqueue - {}", error::to_string(e));
                backward_stream.error({}, make_error_code(vicodyn_errors::failed_to_retry_enqueue),
                                "failed to retry enqueue");
            }
        }
        return shared_forward_dispatch();
    }

    //    TODO: mutexes are bad here, we need to find a way not to block on retries
    auto retry_unsafe() -> void {
        COCAINE_LOG_INFO(endpoint.logger, "retrying");
        request_context->register_retry();
        if (backward_stream.started()) {
            throw error_t("retry is forbidden - response chunk was sent");
        }
        if (request_context->retry_count() > proxy.balancer->retry_count()) {
            throw error_t("retry is forbidden - maximum number of retries reached");
        }
        if (buffer.overfull()) {
            throw error_t("retry is forbidden - buffer is overfull");
        }
        request_context->add_checkpoint("retry");

        choose_endpoint(buffer.headers(), buffer.event());

        // Forward all stored at the moment buffer. Other parts will be forwarded by callbacks.
        buffer.flush(endpoint.forward_stream);
        request_context->add_checkpoint("after_retry");
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
    max_buffer_size(args.as_object().at("max-buffer-size-kb", 16384U).as_uint()),
    logger(context.log(name))
{
    COCAINE_LOG_DEBUG(logger, "created proxy for app {}", app_name);
    on<event_t>([&](const hpack::headers_t& headers, slot_t::tuple_type&& args,
                    slot_t::upstream_type&& backward_stream) {
        auto event = std::get<0>(args);
        auto dispatch_name = format("{}/{}/streaming", this->name(), event);
        auto dispatch = std::make_shared<vicodyn_dispatch_t>(*this, dispatch_name, backward_stream);
        return result_t(dispatch->enqueue(headers, std::move(event)));
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

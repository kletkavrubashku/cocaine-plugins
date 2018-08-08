#include "cocaine/vicodyn/proxy/proxy.hpp"

#include "cocaine/api/vicodyn/balancer.hpp"
#include "cocaine/format/ptr.hpp"
#include "cocaine/format/peer.hpp"
#include "cocaine/repository/vicodyn/balancer.hpp"

#include "cocaine/vicodyn/proxy/endpoint.hpp"
#include "cocaine/vicodyn/proxy/middlewares.hpp"
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
#include <cocaine/vicodyn/proxy/discardable.hpp>

namespace cocaine {
namespace vicodyn {

namespace ph = std::placeholders;

class vicodyn_dispatch_t : public std::enable_shared_from_this<vicodyn_dispatch_t> {
    proxy_t& proxy_;

    std::shared_ptr<request_context_t> request_context_;

    safe_stream_t backward_stream_;
    discardable_dispatch_t forward_dispatch_;

    endpoint_t endpoint_;

public:
    vicodyn_dispatch_t(proxy_t& proxy, const std::string& name, upstream<app_tag_t> b_stream)
        : proxy_(proxy)
        , request_context_(std::make_shared<request_context_t>(*proxy.logger))
        , backward_stream_(std::move(b_stream))
        , forward_dispatch_(name + "/forward")
        , endpoint_(name + "/backward", proxy.balancer, proxy.buffer_capacity, request_context_, *proxy.logger) {

        catcher_t catch_forward_errors(std::bind(&vicodyn_dispatch_t::on_forward_exception, this, ph::_1));
        forward_dispatch_.on<protocol_t::chunk>().with_middleware(catch_forward_errors)
                        .execute(std::bind(&vicodyn_dispatch_t::on_forward_chunk, this, ph::_1, ph::_2));
        forward_dispatch_.on<protocol_t::choke>().with_middleware(catch_forward_errors)
                        .execute(std::bind(&vicodyn_dispatch_t::on_forward_choke, this, ph::_1));
        forward_dispatch_.on<protocol_t::error>().with_middleware(std::move(catch_forward_errors))
                        .execute(std::bind(&vicodyn_dispatch_t::on_forward_error, this, ph::_1, ph::_2, ph::_3));
        forward_dispatch_.on_discard(std::bind(&vicodyn_dispatch_t::on_forward_exception, this, ph::_1));

        catcher_t catch_backward_errors(std::bind(&vicodyn_dispatch_t::on_backward_exception, this, ph::_1));
        endpoint_.on<protocol_t::chunk>().with_middleware(catch_backward_errors)
                        .execute(std::bind(&vicodyn_dispatch_t::on_backward_chunk, this, ph::_1, ph::_2));
        endpoint_.on<protocol_t::choke>().with_middleware(catch_backward_errors)
                        .execute(std::bind(&vicodyn_dispatch_t::on_backward_choke, this, ph::_1));
        endpoint_.on<protocol_t::error>().with_middleware(catch_backward_errors)
                        .execute(std::bind(&vicodyn_dispatch_t::on_backward_error, this, ph::_1, ph::_2, ph::_3));
        endpoint_.on_discard(std::bind(&vicodyn_dispatch_t::on_backward_exception, this, ph::_1));
    }

    auto enqueue(const hpack::headers_t& headers, std::string&& event) -> std::shared_ptr<dispatch<app_tag_t>> {
        try {
            endpoint_.enqueue(proxy_.app_name, headers, std::move(event));
        } catch (const std::system_error&) {

        }
        return std::shared_ptr<dispatch<app_tag_t>>(shared_from_this(), &forward_dispatch_);
    }

private:
    auto on_forward_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void {
        endpoint_.chunk(headers, std::move(chunk));
        request_context_->add_checkpoint("after_fchunk");
    }

    auto on_forward_choke(const hpack::headers_t& headers) -> void {
        endpoint_.choke(headers);
        request_context_->add_checkpoint("after_fchoke");
    }

    auto on_forward_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
        endpoint_.error(headers, std::move(ec), std::move(msg));
        request_context_->add_checkpoint("after_ferror");
    }

    auto on_forward_exception(const std::system_error&) -> void {
        try {
            //backward_stream_.error();
        } catch (const std::system_error& e) {

        }
        request_context_->add_checkpoint("after_fexception");
    }

    auto on_forward_discard(const std::error_code&) -> void {
        try {
            //backward_stream_.error();
        } catch (const std::system_error& e) {

        }
        request_context_->add_checkpoint("after_fdiscard");
    }

private:
    auto on_backward_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void {
        backward_stream_.chunk(headers, std::move(chunk));
        request_context_->add_checkpoint("after_bchunk");
    }

    auto on_backward_choke(const hpack::headers_t& headers) -> void {
        backward_stream_.choke(headers);
        request_context_->add_checkpoint("after_bchoke");
        request_context_->finish();
    }

    auto on_backward_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
        backward_stream_.error(headers, std::error_code(ec), std::string(msg));
        request_context_->add_checkpoint("after_berror");
        request_context_->fail(ec, msg);
    };

    auto on_backward_exception(const std::system_error&) -> void {
        try {
            //endpoint_.error();
        } catch (const std::system_error& e) {

        }
        request_context_->add_checkpoint("after_bexception");
    }

    auto on_backward_discard(const std::error_code&) -> void {
        try {
            //endpoint_.error();
        } catch (const std::system_error& e) {

        }
        request_context_->add_checkpoint("after_bdiscard");
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

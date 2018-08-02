#include "cocaine/vicodyn/balancer/simple.hpp"

#include <cocaine/context.hpp>
#include <cocaine/format/peer.hpp>
#include <cocaine/format/ptr.hpp>

namespace cocaine {
namespace vicodyn {
namespace balancer {

simple_t::simple_t(context_t& ctx, peers_t& peers, asio::io_service& loop, const std::string& app_name,
                   const dynamic_t& args, const dynamic_t::object_t& locator_extra) :
    api::vicodyn::balancer_t(ctx, peers, loop, app_name, args, locator_extra),
    peers(peers),
    logger(ctx.log(format("balancer/simple/{}", app_name))),
    args(args),
    _retry_count(args.as_object().at("retry_count", 4u).as_uint()),
    app_name(app_name),
    x_cocaine_cluster(locator_extra.at("x-cocaine-cluster", "").as_string()),
    ban_timeout(args.as_object().at("ban-timeout-ms", 0U).as_uint())
{
    COCAINE_LOG_INFO(logger, "created simple balancer for app {}", app_name);
}

auto simple_t::choose_peer(const std::shared_ptr<request_context_t>& request_context,
                const hpack::headers_t& /*headers*/, const std::string& /*event*/) -> std::shared_ptr<peer_t> {
    auto peer_predicate = [&](const peer_t& peer) {
        return  peer.connected() &&
                peer.x_cocaine_cluster() == x_cocaine_cluster &&
                !request_context->peer_use_count(peer.uuid());
    };
    auto app_service_predicate = [&](const peers_t::app_service_t& app_service) {
        return !app_service.banned();
    };
    auto peer = peers.choose_random(app_name, peer_predicate, app_service_predicate);
    if (!peer) {
        throw error_t("no peers found");
    }
    return peer;
}

auto simple_t::retry_count() -> size_t {
    return _retry_count;
}

auto simple_t::on_error(const peer_t& peer, std::error_code ec, const std::string& msg) -> void {
    COCAINE_LOG_WARNING(logger, "peer errored - {}({})", ec.message(), msg);
    if(ec.category() == error::node_category() && ec.value() == error::node_errors::not_running) {
        peers.erase_app(peer.uuid(), app_name);
    }
    if(ec.category() == error::overseer_category() && ec.value() == error::queue_is_full) {
        peers.ban_app(peer.uuid(), app_name, ban_timeout);
        COCAINE_LOG_WARNING(logger, "queue is full, peer banned for {}ms - {}", ban_timeout.count(), peer);
    }
}

auto simple_t::is_recoverable(std::error_code ec) -> bool {
    bool queue_is_full = (ec.category() == error::overseer_category() && ec.value() == error::queue_is_full);
    bool unavailable = (ec.category() == error::node_category() && ec.value() == error::not_running);
    bool disconnected = (ec.category() == error::dispatch_category() && ec.value() == error::not_connected);
    return queue_is_full || unavailable || disconnected;
}

} // namespace balancer
} // namespace vicodyn
} // namespace cocaine

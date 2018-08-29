#include "cocaine/gateway/vicodyn.hpp"

#include "../../node/include/cocaine/idl/node.hpp"

#include "cocaine/vicodyn/proxy/proxy.hpp"

#include <cocaine/context.hpp>
#include <cocaine/context/quote.hpp>
#include <cocaine/context/signal.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/dynamic/constructors/endpoint.hpp>
#include <cocaine/dynamic/constructors/set.hpp>
#include <cocaine/dynamic/constructors/shared_ptr.hpp>
#include <cocaine/format/endpoint.hpp>
#include <cocaine/format/exception.hpp>
#include <cocaine/format/vector.hpp>
#include <cocaine/idl/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/memory.hpp>
#include <cocaine/repository.hpp>
#include <cocaine/repository/gateway.hpp>
#include <cocaine/rpc/actor.hpp>
#include <cocaine/traits/dynamic.hpp>
#include <cocaine/traits/endpoint.hpp>
#include <cocaine/traits/vector.hpp>

#include <blackhole/logger.hpp>

namespace cocaine {

template<>
struct dynamic_constructor<vicodyn::peers_t::peer_data_t> {
    static const bool enable = true;

    static inline
    void
    convert(const vicodyn::peers_t::peer_data_t& from, dynamic_t::value_t& to) {
        dynamic_t::object_t data;
        data["extra"] = from.peer->extra();
        data["connected"] = from.peer->connected();
        data["endpoints"] = from.peer->endpoints();
        data["last_active"] = std::chrono::system_clock::to_time_t(from.peer->last_active());
        data["uuid"] = from.peer->uuid();
        data["system_weight"] = from.system_weight.load();
        to = detail::dynamic::incomplete_wrapper<dynamic_t::object_t>();
        boost::get<detail::dynamic::incomplete_wrapper<dynamic_t::object_t>>(to).set(std::move(data));
    }
};

} // namespace cocaine


namespace cocaine {
namespace gateway {

namespace ph = std::placeholders;

vicodyn_t::proxy_description_t::proxy_description_t(std::unique_ptr<tcp_actor_t> _actor, vicodyn::proxy_t& _proxy) :
        actor(std::move(_actor)),
        proxy(_proxy),
        cached_endpoints(this->actor->endpoints())
{}

auto vicodyn_t::proxy_description_t::endpoints() const -> const std::vector<asio::ip::tcp::endpoint>& {
    return cached_endpoints;
}

auto vicodyn_t::proxy_description_t::protocol() const -> const io::graph_root_t& {
    return proxy.root();
}

auto vicodyn_t::proxy_description_t::version() const -> unsigned int {
    return proxy.version();
}

auto vicodyn_t::create_wrapped_gateway() -> void {
    auto wrapped_conf = args.as_object().at("wrapped", dynamic_t::empty_object).as_object();
    auto wrapped_name = wrapped_conf.at("type", "adhoc").as_string();
    auto wrapped_args = wrapped_conf.at("args", dynamic_t::empty_object);
    wrapped_gateway = context.repository().get<gateway_t>(wrapped_name, context, local_uuid, "vicodyn/wrapped", wrapped_args, locator_extra);
}

vicodyn_t::vicodyn_t(context_t& _context, const std::string& _local_uuid, const std::string& name, const dynamic_t& args,
                     const dynamic_t::object_t& locator_extra) :
    gateway_t(_context, _local_uuid, name, args, locator_extra),
    context(_context),
    locator_extra(locator_extra),
    wrapped_gateway(),
    peers(context, args),
    darkmetrics(context, peers, args.as_object().at("darkmetrics", dynamic_t::empty_object)),
    args(args),
    local_uuid(_local_uuid),
    logger(context.log(format("gateway/{}", name)))
{
    create_wrapped_gateway();
    COCAINE_LOG_INFO(logger, "created wrapped gateway");
    auto d = std::make_unique<dispatch<io::vicodyn_tag>>("vicodyn");
    d->on<io::vicodyn::peers>([&](std::string uuid) {
        dynamic_t result = dynamic_t::empty_object;
        dynamic_t& peers_result = result.as_object()["peers"];

        peers.apply_shared([&](const vicodyn::peers_t::data_t& data) mutable {
            if(uuid.empty()) {
                peers_result = data.peers;
            } else {
                auto it = data.peers.find(uuid);
                if(it != data.peers.end()) {
                    peers_result = dynamic_t::empty_object;
                    peers_result.as_object()[uuid] = it->second;
                }
            }
        });
        return result;
    });
    d->on<io::vicodyn::apps>([&](std::string app) {
        dynamic_t result = dynamic_t::empty_object;
        dynamic_t& app_result = result.as_object()["apps"];
        peers.apply_shared([&](const vicodyn::peers_t::data_t& data) mutable {
            auto convert_app_services = [&](const vicodyn::peers_t::app_services_t& from) {
                dynamic_t app_services_result = dynamic_t::empty_object;
                for (const auto& app_service : from) {
                    dynamic_t& peer_result = app_services_result.as_object()[app_service.first];

                    auto banned_for_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            app_service.second.banned_for());
                    if (banned_for_ms > std::chrono::milliseconds::zero()) {
                        peer_result.as_object()["banned_for_ms"] = banned_for_ms.count();
                    }

                    auto positive_request_duration = std::max(app_service.second.avg_request_duration(),
                            std::chrono::nanoseconds(1));
                    // Division by a million is used instead duration_cast for better precision
                    peer_result.as_object()["request_duration_ms"] = positive_request_duration.count() / 1000000.;

                    auto system_weight = vicodyn::defaults::system_weight;
                    auto peer_it = data.peers.find(app_service.first);
                    if (peer_it != data.peers.end()) {
                        system_weight = peer_it->second.system_weight.load();
                    }
                    peer_result.as_object()["system_weight"] = system_weight;
                    peer_result.as_object()["total_weight"] = system_weight / positive_request_duration.count();
                    peer_result.as_object()["x_cocaine_cluster"] = peer_it->second.peer->x_cocaine_cluster();
                }
                return app_services_result;
            };

            if(app.empty()) {
                for (const auto& app : data.apps) {
                    app_result.as_object()[app.first] = convert_app_services(app.second);
                }
            } else {
                auto it = data.apps.find(app);
                if(it != data.apps.end()) {
                    app_result = dynamic_t::empty_object;
                    app_result.as_object()[it->first] = convert_app_services(it->second);
                }
            }
        });
        return result;
    });
    d->on<io::vicodyn::info>([]() -> dynamic_t {
        throw error_t("not implemented");
    });
    COCAINE_LOG_INFO(logger, "created dispatch");
    auto actor = std::make_unique<tcp_actor_t>(context, std::move(d));
    COCAINE_LOG_INFO(logger, "created vicodyn actor");
    context.insert("vicodyn", std::move(actor));
    COCAINE_LOG_DEBUG(logger, "created vicodyn service for  {} with local uuid {}", name, local_uuid);
}

vicodyn_t::~vicodyn_t() {
    COCAINE_LOG_DEBUG(logger, "shutting down vicodyn gateway");
    for (auto& proxy_pair: mapping.unsafe()) {
        proxy_pair.second.actor->terminate();
        COCAINE_LOG_INFO(logger, "stopped {} virtual service", proxy_pair.first);
    }
    mapping.unsafe().clear();
}

auto vicodyn_t::resolve(const std::string& name) const -> service_description_t {
    return mapping.apply([&](const proxy_map_t& mapping){
        auto it = mapping.find(name);
        if(it != mapping.end()) {
            COCAINE_LOG_INFO(logger, "providing virtual app service {} on {}, {}", name, it->second.endpoints(), mapping.size());
            return service_description_t{it->second.endpoints(), it->second.protocol(), it->second.version()};
        }

        static const io::graph_root_t app_root = io::traverse<io::app_tag>().get();
        const auto local = context.locate(name);
        if(local) {
            COCAINE_LOG_DEBUG(logger, "providing local non-app service");
            auto version = static_cast<unsigned int>(local->prototype->version());
            return service_description_t {local->endpoints, local->prototype->root(), version};
        }

        COCAINE_LOG_INFO(logger, "resolving non-app service via wrapped gateway");
        return wrapped_gateway->resolve(name);

    });
}

auto vicodyn_t::consume(const std::string& uuid,
                        const std::string& name,
                        unsigned int version,
                        const std::vector<asio::ip::tcp::endpoint>& endpoints,
                        const io::graph_root_t& protocol,
                        const dynamic_t::object_t& extra) -> void
{
    static const io::graph_root_t node_protocol = io::traverse<io::node_tag>().get();
    static const io::graph_root_t app_protocol = io::traverse<io::app_tag>().get();
    wrapped_gateway->consume(uuid, name, version, endpoints, protocol, extra);
    mapping.apply([&](proxy_map_t& mapping){
        if(name == "node") {
            auto peer = peers.register_peer(uuid, endpoints, extra);
            darkmetrics.subscribe(peer->x_cocaine_cluster());
            COCAINE_LOG_INFO(logger, "registered node service {} with uuid {}", name, uuid);
        } else if (protocol == app_protocol) {
            peers.register_app(uuid, name);
            auto it = mapping.find(name);
            if(it == mapping.end()) {
                auto proxy = std::make_unique<vicodyn::proxy_t>(context, executor.asio(), peers, "virtual::" + name, args, locator_extra);
                auto& proxy_ref = *proxy;
                auto actor = std::make_unique<tcp_actor_t>(context, std::move(proxy));
                actor->run();
                mapping.emplace(name, proxy_description_t(std::move(actor), proxy_ref));
                COCAINE_LOG_INFO(logger, "created new virtual service {}", name);
            } else {
                COCAINE_LOG_INFO(logger, "registered app {} on uuid {} in existing virtual service", name, uuid);
            }
        } else {
            COCAINE_LOG_INFO(logger, "delegating unknown protocol service {} with uuid {} to wrapped gateway", name, uuid);
            return;
        }
    });
    COCAINE_LOG_INFO(logger, "exposed {} service to vicodyn", name);
}

auto vicodyn_t::cleanup(const std::string& uuid, const std::string& name) -> void {
    if(name == "node") {
        peers.erase_peer(uuid);
        COCAINE_LOG_INFO(logger, "dropped node service on {}", uuid);
    } else {
        peers.erase_app(uuid, name);
        mapping.apply([&](proxy_map_t& mapping){
            auto it = mapping.find(name);
            if(it != mapping.end()) {
                if(it->second.proxy.empty()) {
                    it->second.actor->terminate();
                    mapping.erase(it);
                }
            }
        });
    }
    wrapped_gateway->cleanup(uuid, name);
    COCAINE_LOG_INFO(logger, "dropped service {} on {}", name, uuid);
}

auto vicodyn_t::cleanup(const std::string& uuid) -> void {
    peers.erase(uuid);
    mapping.apply([&](proxy_map_t& mapping){
        for(auto it = mapping.begin(); it != mapping.end();) {
            if(it->second.proxy.empty()) {
                it->second.actor->terminate();
                it = mapping.erase(it);
            } else {
                it++;
            }
        }
    });
    wrapped_gateway->cleanup(uuid);
    COCAINE_LOG_INFO(logger, "fully dropped uuid {}", uuid);
}

auto vicodyn_t::total_count(const std::string& name) const -> size_t {
    return mapping.apply([&](const proxy_map_t& mapping) -> size_t {
        auto it = mapping.find(name);
        if(it == mapping.end()) {
            return wrapped_gateway->total_count(name);
        }
        return it->second.proxy.size();
    });
}

} // namespace gateway
} // namespace cocaine

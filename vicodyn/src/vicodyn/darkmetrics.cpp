#include "darkmetrics.hpp"

#include <cocaine/context.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/format/exception.hpp>
#include <cocaine/locked_ptr.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/unicorn/value.hpp>

#include <blackhole/logger.hpp>

namespace cocaine {
namespace vicodyn {

namespace ph = std::placeholders;

class darkmetrics_t::subscription_t {
    using safe_bool = synchronized<bool>;

    const std::string cluster_;
    const std::string path_;
    const std::chrono::seconds ttl_;
    const std::chrono::seconds retry_after_;
    api::unicorn_scope_ptr scope_;
    asio::deadline_timer expiration_timer_;
    asio::deadline_timer retry_timer_;

    api::unicorn_t& unicorn_;
    peers_t& peers_;
    logging::logger_t& logger_;

    std::shared_ptr<safe_bool> terminating_ = std::make_shared<safe_bool>(false);

public:
    subscription_t(const std::string& cluster, const std::string& unicorn_prefix, peers_t& peers,
            std::chrono::seconds system_weights_ttl, std::chrono::seconds retry_after, api::unicorn_t& unicorn,
            executor::owning_asio_t& executor, logging::logger_t& logger)
        : cluster_(cluster)
        , path_(format("{}/{}/metrics", unicorn_prefix, cluster))
        , ttl_(system_weights_ttl)
        , retry_after_(retry_after)
        , expiration_timer_(executor.asio())
        , retry_timer_(executor.asio())
        , unicorn_(unicorn)
        , peers_(peers)
        , logger_(logger) {

        subscribe();
        schedule_expiration();
        COCAINE_LOG_INFO(logger_, "subscribed to metrics by path `{}`", path_);
    }

    ~subscription_t() {
        // Wait for finish of timer callbacks and disable them
        *terminating_->synchronize() = true;
        scope_->close();
    }


private:
    auto subscribe() -> void {
        if (scope_) {
            scope_->close();
        }
        scope_ = unicorn_.subscribe(std::bind(&subscription_t::on_subscribe, this, ph::_1), path_);
    }

    auto on_subscribe(std::future<unicorn::versioned_value_t> future) -> void {
        /*** Message format:
        {
            "timestamp": 1533922332,
            "nodes": {
                "121170e8-a830-4239-8875-8d18307a920b": { "system_usable_weight": 0.05964477254440246 }
            }
        }
        ***/
        try {
            const auto versioned_value = future.get();
            if (versioned_value.version() == unicorn::not_existing_version) {
                COCAINE_LOG_WARNING(logger_, "metrics for `{}` don't exist", cluster_);
                return;
            }
            const auto& value = versioned_value.value();

            using clock_t = std::chrono::system_clock;
            const std::chrono::seconds timestamp_s(value.as_object()["timestamp"].as_uint());
            if (clock_t::time_point(timestamp_s) + ttl_ < clock_t::now()) {
                COCAINE_LOG_WARNING(logger_, "metrics for `{}` is too old timestamp={}s, ttl={}s",
                        cluster_, timestamp_s.count(), ttl_.count());
                return;
            }

            const auto& nodes = value.as_object()["nodes"].as_object();
            COCAINE_LOG_INFO(logger_, "received metrics for {} nodes on `{}`", nodes.size(), cluster_);
            // Using of method apply_shared_unsafe is correct because peer_data_t::system_weight is atomic value
            peers_.apply_shared_unsafe([&](peers_t::data_t& data) {
                for (const auto& peer : nodes) {
                    const auto system_usable_weight = peer.second.as_object()["system_usable_weight"].as_double();
                    auto peers_it = data.peers.find(peer.first);
                    if (peers_it == data.peers.end()) {
                        continue;
                    }
                    peers_it->second.system_weight = system_usable_weight;
                    COCAINE_LOG_DEBUG(logger_, "metrics for peer {}: system_usable_weight={} timestamp={}",
                            peer.first, system_usable_weight, timestamp_s.count());
                }
            });

            schedule_expiration();
        } catch (const std::exception& err) {
            COCAINE_LOG_ERROR(logger_, "failed to handle update for `{}` - {}", cluster_, err.what());
            schedule_retry();
        }
    }

    auto schedule_expiration() -> void {
        expiration_timer_.expires_from_now(boost::posix_time::seconds(ttl_.count()));
        expiration_timer_.async_wait([&, terminating = terminating_](const std::error_code& ec) {
            if (ec) {
                return;
            }
            auto locked_terminating = terminating->synchronize();
            if (*locked_terminating) {
                return;
            }

            COCAINE_LOG_WARNING(logger_, "timer expired, set system weight to {} for all peers in `{}`",
                    defaults::system_weight, cluster_);
            // Using of method apply_shared_unsafe is correct because peer_data_t::system_weight is atomic value
            peers_.apply_shared_unsafe([&](peers_t::data_t& data) {
                for (auto& peer_data : data.peers) {
                    if (peer_data.second.peer->x_cocaine_cluster() != cluster_) {
                        continue;
                    }
                    peer_data.second.system_weight = defaults::system_weight;
                }
            });
        });
    }

    auto schedule_retry() -> void {
        retry_timer_.expires_from_now(boost::posix_time::seconds(retry_after_.count()));
        retry_timer_.async_wait([&, terminating = terminating_](const std::error_code& ec) {
            if (ec) {
                return;
            }
            auto locked_terminating = terminating->synchronize();
            if (*locked_terminating) {
                return;
            }

            subscribe();
            COCAINE_LOG_INFO(logger_, "resubscribed to metrics by path `{}`", path_);
        });
    }
};

darkmetrics_t::darkmetrics_t(context_t& context, peers_t& peers, const dynamic_t& args)
    : unicorn_name_(args.as_object().at("unicorn", dynamic_t::empty_string).as_string())
    , unicorn_prefix_(args.as_object().at("unicorn_prefix", dynamic_t::empty_string).as_string())
    , unicorn_retry_after_(args.as_object().at("unicorn_retry_after_s", 20U).as_uint())
    , enabled_(args.as_object().at("enabled", false).as_bool())
    , ttl_(args.as_object().at("ttl_s", 300U).as_uint())
    , logger_(context.log("vicodyn/darkmetrics"))
    , peers_(peers) {

    if (enabled_) {
        if (unicorn_name_.empty() || unicorn_prefix_.empty()) {
            throw error_t("invalid configuration of darkmetrics: `unicorn` and `unicorn_prefix` are required");
        }
        unicorn_ = api::unicorn(context, unicorn_name_);
    }
    COCAINE_LOG_INFO(logger_, "balancing by system weights is {}", enabled_ ? "on" : "off");
}

darkmetrics_t::~darkmetrics_t() = default;

auto darkmetrics_t::subscribe(const std::string& cluster) -> void {
    if (enabled_ && clusters_.emplace(cluster).second) {
        subscriptions_.emplace_back(cluster, unicorn_prefix_, peers_, ttl_, unicorn_retry_after_,
                *unicorn_, executor_, *logger_);
    }
}

} // namespace vicodyn
} // namespace cocaine

#pragma once

#include <cocaine/api/unicorn.hpp>
#include <cocaine/forwards.hpp>
#include <cocaine/vicodyn/peer.hpp>

#include <unordered_set>

namespace cocaine {
namespace vicodyn {

class darkmetrics_t {
    class subscription_t;

    const std::string unicorn_name_;
    const std::string unicorn_prefix_;
    const std::chrono::seconds unicorn_retry_after_;

    const bool enabled_;
    const std::chrono::seconds ttl_;

    const std::unique_ptr<logging::logger_t> logger_;
    executor::owning_asio_t executor_;
    peers_t& peers_;

    api::unicorn_ptr unicorn_;
    std::unordered_set<std::string> clusters_;
    std::list<subscription_t> subscriptions_;

public:
    darkmetrics_t(context_t& context, peers_t& peers, const dynamic_t& args);
    ~darkmetrics_t();

    auto subscribe(const std::string& cluster) -> void;
};

} // namespace vicodyn
} // namespace cocaine

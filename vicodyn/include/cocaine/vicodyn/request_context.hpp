#pragma once

#include "cocaine/vicodyn/peer.hpp"

#include <blackhole/logger.hpp>

#include <boost/any.hpp>

namespace cocaine {
namespace vicodyn {

class request_context_t: public std::enable_shared_from_this<request_context_t> {
    using clock_t = std::chrono::system_clock;
    struct checkpoint_t {
        const char* message;
        size_t msg_len;
        clock_t::time_point when;
    };

    blackhole::logger_t& logger_;
    clock_t::time_point start_time_;
    std::atomic_flag closed_;

    using peer_array_t = std::vector<std::shared_ptr<peer_t>>;
    using checkpoint_array_t = std::vector<checkpoint_t>;
    using counter_map_t = std::unordered_map<std::string, std::atomic<std::size_t>>;

    synchronized<peer_array_t> used_peers_;
    synchronized<checkpoint_array_t> checkpoints_;
    synchronized<counter_map_t> counters_;

public:
    request_context_t(blackhole::logger_t& logger);

    ~request_context_t();

    auto mark_used_peer(std::shared_ptr<peer_t> peer) -> void;

    auto peer_use_count(const std::shared_ptr<peer_t>& peer) -> size_t;

    auto peer_use_count(const std::string& peer_uuid) -> size_t;

    auto counter(const std::string& name) -> std::atomic<std::size_t>&;

    template <size_t N>
    auto add_checkpoint(const char(&name)[N]) -> void {
        checkpoints_->emplace_back(checkpoint_t{name, N - 1, std::chrono::system_clock::now()});
    }

    auto finish() -> void;

    auto fail(const std::error_code& ec, blackhole::string_view reason) -> void;

private:
    auto current_duration_ms() -> size_t;

    auto write(int level, const std::string& msg) -> void;
};

} // namespace vicodyn
} // namespace cocaine

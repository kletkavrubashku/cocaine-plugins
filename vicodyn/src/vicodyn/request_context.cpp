#include "cocaine/vicodyn/request_context.hpp"

#include <cocaine/logging.hpp>

namespace cocaine {
namespace vicodyn {

request_context_t::request_context_t(blackhole::logger_t& logger) :
    logger_(logger),
    start_time_(clock_t::now()),
    closed_(ATOMIC_FLAG_INIT)
{}

request_context_t::~request_context_t() {
    fail({}, "dtor called");
}

auto request_context_t::mark_used_peer(std::shared_ptr<peer_t> peer) -> void {
    used_peers_->emplace_back(std::move(peer));
}

auto request_context_t::peer_use_count(const std::shared_ptr<peer_t>& peer) -> size_t {
    return used_peers_.apply([&](const std::vector<std::shared_ptr<peer_t>>& used_peers_){
        return std::count(used_peers_.begin(), used_peers_.end(), peer);
    });
}

auto request_context_t::peer_use_count(const std::string& peer_uuid) -> size_t {
    return used_peers_.apply([&](const std::vector<std::shared_ptr<peer_t>>& used_peers){
        return std::count_if(used_peers.begin(), used_peers.end(), [&](const std::shared_ptr<peer_t>& peer){
            return peer->uuid() == peer_uuid;
        });
    });
}

auto request_context_t::counter(const std::string& name) -> std::atomic<std::size_t>& {
    return (*counters_.synchronize())[name];
}

auto request_context_t::finish() -> void {
    static std::string msg("finished request");
    write(logging::info, "finished request");
}

auto request_context_t::fail(const std::error_code& ec, blackhole::string_view reason) -> void {
    write(logging::warning, format("finished request with error {} - {}", ec, reason));
}

auto request_context_t::current_duration_ms() -> size_t {
    auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(clock_t::now() - start_time_);
    return dur.count();
}

auto request_context_t::write(int level, const std::string& msg) -> void {
    if(closed_.test_and_set()) {
        return;
    }
    add_checkpoint("total_duration_ms");

    blackhole::view_of<blackhole::attributes_t>::type view;
    used_peers_.apply([&](const peer_array_t & peers) {
        for (const auto& peer: peers) {
            view.emplace_back("peer", peer->uuid());
        }
    });
    checkpoints_.apply([&](const checkpoint_array_t & checkpoints) {
        for (const auto& checkpoint: checkpoints) {
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(checkpoint.when - start_time_);
            view.emplace_back(blackhole::string_view(checkpoint.message, checkpoint.msg_len), ms.count());
        }
    });
    counters_.apply([&](const counter_map_t& counters) {
        for (const auto& counter: counters) {
            view.emplace_back(counter.first, counter.second.load());
        }
    });
    COCAINE_LOG(logger_, logging::priorities(level), msg, view);
}

} // namespace vicodyn
} // namespace cocaine

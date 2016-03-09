#include "cocaine/detail/service/node/dispatch/client.hpp"

using namespace cocaine;

client_rpc_dispatch_t::client_rpc_dispatch_t(const std::string& name):
    dispatch<incoming_tag>(format("%s/C2W", name)),
    state(state_t::open)
{
    // Uncaught exceptions here will lead to a client disconnection and further dispatch discarding.

    on<protocol::chunk>([&](const std::string& chunk) {
        write({}, chunk);
    });

    on<protocol::error>([&](const std::error_code& ec, const std::string& reason) {
        abort({}, ec, reason);
    });

    on<protocol::choke>([&] {
        close({});
    });
}

void
client_rpc_dispatch_t::attach(upstream<outcoming_tag> stream_, callback_type callback_) {
    std::lock_guard<std::mutex> lock(mutex);

    stream().attach(std::move(stream_));

    switch (state) {
    case state_t::open:
        state = state_t::bound;
        callback = callback_;
        break;
    case state_t::closed:
        callback_(ec);
        break;
    default:
        BOOST_ASSERT(false);
    }
}

void
client_rpc_dispatch_t::discard(const std::error_code& ec) const {
    // TODO: Consider something less weird.
    const_cast<client_rpc_dispatch_t*>(this)->discard(ec);
}

void
client_rpc_dispatch_t::discard(const std::error_code& ec) {
    if (ec) {
        this->ec = ec;

        try {
            // TODO: Add category to indicate that the error is generated by the core.
            stream().abort({}, ec, ec.message());
        } catch (const std::exception&) {
            // Eat.
        }

        finalize();
    }
}

auto client_rpc_dispatch_t::write(hpack::header_storage_t headers, const std::string& data) -> void {
    stream().write(std::move(headers), data);
}

auto client_rpc_dispatch_t::abort(hpack::header_storage_t headers, const std::error_code& ec, const std::string& reason) -> void {
    stream().abort(std::move(headers), ec, reason);
    finalize();
}

auto client_rpc_dispatch_t::close(hpack::header_storage_t headers) -> void {
    stream().close(std::move(headers));
    finalize();
}

auto client_rpc_dispatch_t::stream() -> streamed<std::string>& {
    return data.stream;
}

void
client_rpc_dispatch_t::finalize() {
    std::lock_guard<std::mutex> lock(mutex);

    switch (state) {
    case state_t::open:
        state = state_t::closed;
        break;
    case state_t::bound:
        state = state_t::closed;
        callback(ec);
        break;
    case state_t::closed:
        break;
    default:
        BOOST_ASSERT(false);
    }
}

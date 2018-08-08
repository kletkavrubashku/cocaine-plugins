#pragma once

#include <cocaine/rpc/dispatch.hpp>
#include <cocaine/rpc/upstream.hpp>

namespace cocaine {
namespace vicodyn {

using app_tag_t = io::stream_of<std::string>::tag;
using protocol_t = io::protocol<app_tag_t>::scope;

} // namespace vicodyn
} // namespace cocaine
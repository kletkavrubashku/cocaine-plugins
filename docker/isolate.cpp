/*
    Copyright (c) 2011-2013 Andrey Goryachev <andrey.goryachev@gmail.com>
    Copyright (c) 2011-2013 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "isolate.hpp"

#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/memory.hpp>

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <system_error>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/lexical_cast.hpp>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace cocaine;
using namespace cocaine::isolate;

namespace fs = boost::filesystem;

namespace {

struct container_handle_t:
    public api::handle_t
{
    COCAINE_DECLARE_NONCOPYABLE(container_handle_t)

    container_handle_t(const docker::container_t& container):
        m_container(container),
        m_terminated(false)
    { }

    virtual
   ~container_handle_t() {
        terminate();
    }

    void
    start(const std::vector<std::string>& binds) {
        m_container.start(binds);
    }

    void
    attach() {
        m_connection = m_container.attach();
    }

    virtual
    void
    terminate() {
        if(!m_terminated) {
            try {
                m_container.kill();
            } catch(...) {
                // pass
            }

            try {
                m_container.remove();
            } catch (...) {
                // pass
            }

            m_terminated = true;
        }
    }

    virtual
    int
    stdout() const {
        return m_connection.fd();
    }

private:
    docker::container_t m_container;
    docker::connection_t m_connection;
    bool m_terminated;
};

} // namespace

docker_t::docker_t(context_t& context, const std::string& name, const dynamic_t& args):
    category_type(context, name, args),
    m_log(new logging::log_t(context, name)),
    m_do_pool(false),
    m_docker_client(
        docker::endpoint_t::from_string(args.as_object().at("endpoint", "unix:///var/run/docker.sock").as_string()),
        m_log
    )
{
    const auto& config = args.as_object();
    m_runtime_path = config.at("runtime-path", "/run/cocaine").as_string();

    if(config.count("registry") > 0) {
        m_do_pool = true;

        // <default> means default docker's registry (the one owned by dotCloud). User must specify it explicitly.
        // I think that if there is no registry in the config then the plugin must not pull images from foreign registry by default.
        if(config["registry"].as_string() != "<default>") {
            m_image += config["registry"].as_string() + "/";
        }
    } else {
        COCAINE_LOG_WARNING(m_log, "Docker registry is not specified. Only local images will be used.");
    }

    if(config.count("repository") > 0) {
        m_image += config["repository"].as_string() + "/";
    }
    m_image += name;
    m_tag = ""; // empty for now

    m_run_config.SetObject();

    m_run_config.AddMember("Hostname", "", m_json_allocator);
    m_run_config.AddMember("User", "", m_json_allocator);
    m_run_config.AddMember("Memory", config.at("memory_limit", 0).to<unsigned>(), m_json_allocator);
    m_run_config.AddMember("MemorySwap", config.at("memory_swap", 0).to<unsigned>(), m_json_allocator);
    m_run_config.AddMember("CpuShares", config.at("cpu_shares", 0).to<unsigned>(), m_json_allocator);
    m_run_config.AddMember("AttachStdin", false, m_json_allocator);
    m_run_config.AddMember("AttachStdout", false, m_json_allocator);
    m_run_config.AddMember("AttachStderr", false, m_json_allocator);
    rapidjson::Value v1;
    m_run_config.AddMember("PortSpecs", v1, m_json_allocator);
    m_run_config.AddMember("Tty", false, m_json_allocator);
    m_run_config.AddMember("OpenStdin", false, m_json_allocator);
    m_run_config.AddMember("StdinOnce", false, m_json_allocator);
    rapidjson::Value v2(rapidjson::kArrayType);
    m_run_config.AddMember("Env", v2, m_json_allocator);
    rapidjson::Value v3(rapidjson::kArrayType);
    m_run_config.AddMember("Cmd", v3, m_json_allocator);
    rapidjson::Value v4;
    m_run_config.AddMember("Dns", v4, m_json_allocator);
    m_run_config.AddMember("Image", m_image.data(), m_json_allocator);
    rapidjson::Value v5(rapidjson::kObjectType);
    m_run_config.AddMember("Volumes", v5, m_json_allocator);
    rapidjson::Value empty_object(rapidjson::kObjectType);
    m_run_config["Volumes"].AddMember(m_runtime_path.c_str(), empty_object, m_json_allocator);
    m_run_config.AddMember("VolumesFrom", "", m_json_allocator);
    m_run_config.AddMember("WorkingDir", "/", m_json_allocator);
}

docker_t::~docker_t() {
    // pass
}

void
docker_t::spool() {
    if(m_do_pool) {
        m_docker_client.pull_image(m_image, m_tag);
    }
}

std::unique_ptr<api::handle_t>
docker_t::spawn(const std::string& path, const api::string_map_t& args, const api::string_map_t& environment) {
    // prepare request to docker
    auto& env = m_run_config["Env"];
    env.SetArray();
    for(auto it = environment.begin(); it != environment.end(); ++it) {
        env.PushBack((it->first + "=" + it->second).c_str(), m_json_allocator);
    }

    auto& cmd = m_run_config["Cmd"];

    cmd.SetArray();
    cmd.PushBack(path.c_str(), m_json_allocator);

    fs::path endpoint = fs::path(m_runtime_path);
    for (auto it = args.begin(); it != args.end(); ++it) {
        cmd.PushBack(it->first.c_str(), m_json_allocator);
        if(it->first == "--endpoint") {
            endpoint /= fs::path(it->second).filename();
#if BOOST_VERSION >= 104600
            cmd.PushBack(endpoint.native().c_str(), m_json_allocator);
#else
            cmd.PushBack(endpoint.string().c_str(), m_json_allocator);
#endif
        } else {
            cmd.PushBack(it->second.c_str(), m_json_allocator);
        }
    }

    std::vector<std::string> binds;
#if BOOST_VERSION >= 104600
    std::string socket_dir(fs::path(args.at("--endpoint")).remove_filename().native().c_str());
#else
    std::string socket_dir(fs::path(args.at("--endpoint")).remove_filename().string().c_str());
#endif
    binds.emplace_back((socket_dir + ":" + m_runtime_path).c_str());

    // create container
    std::unique_ptr<container_handle_t> handle(
        new container_handle_t(m_docker_client.create_container(m_run_config))
    );

    handle->start(binds);
    handle->attach();

    return std::move(handle);
}

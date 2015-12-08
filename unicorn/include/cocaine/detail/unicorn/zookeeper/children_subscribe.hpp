/*
* 2015+ Copyright (c) Anton Matveenko <antmat@yandex-team.ru>
* All rights reserved.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*/

#pragma once

#include "cocaine/unicorn/api.hpp"

#include "cocaine/unicorn/api/zookeeper.hpp"

#include "cocaine/zookeeper/handler.hpp"

namespace cocaine { namespace unicorn {

/**
* Action for handling requests during subscription for childs.
* When client subscribes for a path - we make a get request to ZK with setting watch on specified path.
* On each get completion we compare last sent verison to client with current and if current version is greater send update to client.
* On each watch invoke we issue child command (to later process with this handler) starting new watch.
*/
struct children_subscribe_action_t :
    public zookeeper::managed_strings_stat_handler_base_t,
    public zookeeper::managed_watch_handler_base_t
{
    typedef api::unicorn_t::writable_ptr writable_ptr;

    children_subscribe_action_t(
        const zookeeper::handler_tag& tag,
        writable_ptr::children_subscribe _result,
        const zookeeper_t::context_t& ctx,
        path_t _path
    );

    /**
    * Handling child requests
    */
    virtual void
        operator()(int rc, std::vector<std::string> childs, const zookeeper::node_stat& stat);

    /**
    * Handling watch
    */
    virtual void
        operator()(int type, int state, zookeeper::path_t path);


    writable_ptr::children_subscribe result;
    zookeeper_t::context_t ctx;
    std::mutex write_lock;
    version_t last_version;
    const path_t path;
};

}} //namespace cocaine::unicorn
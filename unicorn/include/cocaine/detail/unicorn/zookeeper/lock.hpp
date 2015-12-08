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

#include "cocaine/detail/unicorn/zookeeper/create.hpp"
#include "cocaine/detail/unicorn/zookeeper/lock_state.hpp"
#include "cocaine/unicorn/api.hpp"
#include "cocaine/unicorn/api/zookeeper.hpp"
#include "cocaine/zookeeper/handler.hpp"

namespace cocaine { namespace unicorn {

/**
 * Lock mechanism is described here:
 * http://zookeeper.apache.org/doc/r3.1.2/recipes.html#sc_recipes_Locks
 */
struct lock_action_t :
    public create_action_base_t,
    public zookeeper::managed_strings_stat_handler_base_t,
    public zookeeper::managed_stat_handler_base_t,
    public zookeeper::managed_watch_handler_base_t
{
    lock_action_t(const zookeeper::handler_tag& tag,
                  const unicorn::zookeeper_t::context_t& ctx,
                  std::shared_ptr<lock_state_t> state,
                  path_t _path,
                  path_t folder,
                  value_t _value,
                  api::unicorn_t::writable_ptr::lock _result);

    /**
    * Childs subrequest handler
    */
    virtual void
    operator()(int rc, std::vector<std::string> childs, zookeeper::node_stat const& stat);

    /**
    * Exists subrequest handler
    */
    virtual void
    operator()(int rc, zookeeper::node_stat const& stat);


    /**
    * Watcher handler to watch on lock release.
    */
    virtual void operator()(int type, int state, unicorn::path_t path);

    /**
    * Implicit call to base.
    */
    virtual void
    operator()(int rc, zookeeper::value_t value) {
        return create_action_base_t::operator()(rc, std::move(value));
    }

    /**
    * Lock creation handler.
    */
    virtual void
    finalize(zookeeper::value_t);

    virtual void
    abort(int rc);

    std::shared_ptr<lock_state_t> state;
    std::weak_ptr<lock_state_t> weak_state;
    api::unicorn_t::writable_ptr::lock result;
    path_t folder;
    std::string created_node_name;
};

/**
* Handler for lock_release.
*/
struct release_lock_action_t :
public zookeeper::void_handler_base_t
{
    release_lock_action_t(const zookeeper_t::context_t& ctx);

    virtual void
    operator()(int rc);

    zookeeper_t::context_t ctx;
};

}} //namespace cocaine::unicorn

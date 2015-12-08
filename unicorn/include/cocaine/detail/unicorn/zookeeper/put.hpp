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

struct put_action_t :
    public zookeeper::managed_stat_handler_base_t,
    public zookeeper::managed_data_handler_base_t
{

    put_action_t(const zookeeper::handler_tag& tag,
                 const zookeeper_t::context_t& ctx,
                 api::unicorn_t::writable_ptr::put _result,
                 unicorn::path_t _path,
                 unicorn::value_t _value,
                 unicorn::version_t _version
    );

    /**
    * handling set request
    */
    virtual void
    operator()(int rc, zookeeper::node_stat const& stat);

    /**
    * handling get request after version mismatch
    */
    virtual void
    operator()(int rc, zookeeper::value_t value, zookeeper::node_stat const& stat);

    zookeeper_t::context_t ctx;
    api::unicorn_t::writable_ptr::put result;
    unicorn::path_t path;
    unicorn::value_t initial_value;
    zookeeper::value_t encoded_value;
    unicorn::version_t version;
};

}} //namespace cocaine::unicorn

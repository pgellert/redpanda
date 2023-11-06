/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

namespace transform {
class service;
template<typename ClockType>
class manager;
class processor;
class probe;
namespace rpc {
class client;
class local_service;
class reporter;
} // namespace rpc
} // namespace transform
/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/metadata_cache.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "config/data_directory_path.h"
#include "config/node_config.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/errors.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "kafka/server/handlers/topics/topic_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/validation.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "reflection/type_traits.h"
#include "security/acl.h"
#include "ssx/sformat.h"
#include "tristate.h"
#include "utils/type_traits.h"

#include <seastar/core/sstring.hh>

#include <fmt/ranges.h>

#include <charconv>
#include <optional>
#include <string_view>
#include <type_traits>

namespace kafka {

bool config_property_requested(
  const std::optional<std::vector<ss::sstring>>& configuration_keys,
  const std::string_view property_name);

template<typename T>
void add_config(
  describe_configs_result& result,
  std::string_view name,
  T value,
  describe_configs_source source);

template<typename T>
void add_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view name,
  T value,
  describe_configs_source source);

template<typename T>
static inline ss::sstring describe_as_string(const T& t) {
    return ssx::sformat("{}", t);
}

template<typename T, typename Func>
void add_broker_config(
  describe_configs_result& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f);

template<typename T, typename Func>
void add_broker_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view name,
  const config::property<T>& property,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f);

template<typename T, typename Func>
void add_topic_config(
  describe_configs_result& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f);

template<typename T, typename Func>
void add_topic_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view default_name,
  const T& default_value,
  std::string_view override_name,
  const std::optional<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation,
  Func&& describe_f,
  bool hide_default_override = false);

template<typename T>
void add_topic_config(
  describe_configs_result& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation);

template<typename T>
void add_topic_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation);

ss::sstring
kafka_endpoint_format(const std::vector<model::broker_endpoint>& endpoints);

ss::sstring kafka_authn_endpoint_format(
  const std::vector<config::broker_authn_endpoint>& endpoints);


void report_topic_config(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties,
  bool include_synonyms,
  bool include_documentation);

std::vector<creatable_topic_configs> make_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_config);

} // namespace kafka

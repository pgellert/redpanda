#pragma once

#include "kafka/protocol/describe_configs.h"

namespace kafka {

template<typename T>
ss::sstring describe_as_string(const T& t);

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
void add_topic_config_if_requested(
  const describe_configs_resource& resource,
  describe_configs_result& result,
  std::string_view default_name,
  const std::optional<T>& default_value,
  std::string_view override_name,
  const tristate<T>& overrides,
  bool include_synonyms,
  std::optional<ss::sstring> documentation);

} // namespace kafka

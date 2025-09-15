/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/time/time.h"
#include "serde/protobuf/base.h"
#include "serde/protobuf/field_mask.h"

#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

namespace redpanda::admin {

/**
 * Field accessor information for a protobuf field.
 * Contains type information and accessor functions for different field types.
 */
template<typename T>
struct FieldAccessorInfo {
    enum Type { Int64, Double, Bool, String, Duration, Timestamp } type;

    // Accessor functions for each supported type (only one will be set based on
    // 'type')
    std::function<int64_t(const T&)> getInt64;
    std::function<double(const T&)> getDouble;
    std::function<bool(const T&)> getBool;
    std::function<std::string(const T&)> getString;
    std::function<absl::Duration(const T&)> getDuration;
    std::function<absl::Time(const T&)> getTimestamp;
};

/**
 * Abstract base interface for field registries.
 * This allows AIPFilterParser to work with any registry implementation.
 */
template<typename T>
class IProtobufFieldRegistry {
public:
    virtual ~IProtobufFieldRegistry() = default;

    /**
     * Check if a field path exists in the registry.
     */
    virtual bool has_field(const std::string& field_path) const = 0;

    /**
     * Get field accessor information for a given field path.
     * @throws std::invalid_argument if field path is not found
     */
    virtual FieldAccessorInfo<T>
    get_field_info(const std::string& field_path) const = 0;

    /**
     * Get all available field paths (optional - may not be efficiently
     * implementable for dynamic registries). Default implementation returns
     * empty vector.
     */
    virtual std::vector<std::string> get_field_paths() const { return {}; }
};

/**
 * Concrete registry that maps field paths to type-safe accessor functions.
 * This registry can be used for filtering, sorting, and other field-based
 * operations.
 */
template<typename T>
class ProtobufFieldRegistry : public IProtobufFieldRegistry<T> {
public:
    using FieldAccessorMap
      = std::unordered_map<std::string, FieldAccessorInfo<T>>;

    explicit ProtobufFieldRegistry(FieldAccessorMap accessors)
      : accessors_(std::move(accessors)) {}

    /**
     * Check if a field path exists in the registry.
     */
    bool has_field(const std::string& field_path) const override {
        return accessors_.find(field_path) != accessors_.end();
    }

    /**
     * Get field accessor information for a given field path.
     * @throws std::invalid_argument if field path is not found
     */
    FieldAccessorInfo<T>
    get_field_info(const std::string& field_path) const override {
        auto it = accessors_.find(field_path);
        if (it == accessors_.end()) {
            throw std::invalid_argument("Unknown field path: " + field_path);
        }
        return it->second;
    }

    /**
     * Get all available field paths.
     */
    std::vector<std::string> get_field_paths() const override {
        std::vector<std::string> paths;
        paths.reserve(accessors_.size());
        for (const auto& [path, _] : accessors_) {
            paths.push_back(path);
        }
        return paths;
    }

    /**
     * Get the underlying accessor map (for advanced use cases).
     */
    const FieldAccessorMap& get_accessors() const { return accessors_; }

private:
    FieldAccessorMap accessors_;
};

/**
 * Builder for creating protobuf field registries with type-safe accessor
 * registration.
 */
template<typename T>
class ProtobufFieldRegistryBuilder {
public:
    using Registry = ProtobufFieldRegistry<T>;
    using FieldAccessorMap = typename Registry::FieldAccessorMap;

    /**
     * Add an int64 field accessor.
     */
    ProtobufFieldRegistryBuilder& addInt64Field(
      const std::string& field_path,
      std::function<int64_t(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Int64,
          std::move(accessor),
          nullptr,
          nullptr,
          nullptr,
          nullptr,
          nullptr};
        return *this;
    }

    /**
     * Add a double field accessor.
     */
    ProtobufFieldRegistryBuilder& addDoubleField(
      const std::string& field_path, std::function<double(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Double,
          nullptr,
          std::move(accessor),
          nullptr,
          nullptr,
          nullptr,
          nullptr};
        return *this;
    }

    /**
     * Add a bool field accessor.
     */
    ProtobufFieldRegistryBuilder& addBoolField(
      const std::string& field_path, std::function<bool(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Bool,
          nullptr,
          nullptr,
          std::move(accessor),
          nullptr,
          nullptr,
          nullptr};
        return *this;
    }

    /**
     * Add a string field accessor.
     */
    ProtobufFieldRegistryBuilder& addStringField(
      const std::string& field_path,
      std::function<std::string(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::String,
          nullptr,
          nullptr,
          nullptr,
          std::move(accessor),
          nullptr,
          nullptr};
        return *this;
    }

    /**
     * Add a duration field accessor (native absl::Duration support).
     */
    ProtobufFieldRegistryBuilder& addDurationField(
      const std::string& field_path,
      std::function<absl::Duration(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Duration,
          nullptr,
          nullptr,
          nullptr,
          nullptr,
          std::move(accessor),
          nullptr};
        return *this;
    }

    /**
     * Add a timestamp field accessor (native absl::Time support).
     */
    ProtobufFieldRegistryBuilder& addTimestampField(
      const std::string& field_path,
      std::function<absl::Time(const T&)> accessor) {
        accessors_[field_path] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Timestamp,
          nullptr,
          nullptr,
          nullptr,
          nullptr,
          nullptr,
          std::move(accessor)};
        return *this;
    }

    /**
     * Build the final registry.
     */
    Registry build() && { return Registry(std::move(accessors_)); }

private:
    FieldAccessorMap accessors_;
};

/**
 * Automatic registry that uses protobuf reflection to dynamically lookup
 * fields. Works with any type that inherits from serde::pb::base_message. This
 * registry looks up fields on-demand instead of requiring pre-registration.
 */
template<typename T>
class AutoProtobufFieldRegistry : public IProtobufFieldRegistry<T> {
public:
    static_assert(
      std::is_base_of_v<serde::pb::base_message, T>,
      "Type must inherit from serde::pb::base_message");

    /**
     * Check if a field path exists and is supported.
     */
    bool has_field(const std::string& field_path) const override {
        try {
            // Try to convert the field path and look up the field
            auto field_numbers_opt = convert_field_path_to_numbers(field_path);
            if (!field_numbers_opt) {
                return false;
            }

            // Check if we can look up the field
            T sample_instance;
            auto field_opt = sample_instance.lookup_field(*field_numbers_opt);
            if (!field_opt) {
                return false;
            }

            // Check if the field type is supported
            return is_field_type_supported(field_opt->value);
        } catch (...) {
            return false;
        }
    }

    /**
     * Get field accessor information for a given field path.
     * @throws std::invalid_argument if field path is not found or unsupported
     */
    FieldAccessorInfo<T>
    get_field_info(const std::string& field_path) const override {
        // Convert field path to field numbers
        auto field_numbers_opt = convert_field_path_to_numbers(field_path);
        if (!field_numbers_opt) {
            throw std::invalid_argument("Invalid field path: " + field_path);
        }

        // Look up the field to determine its type
        T sample_instance;
        auto field_opt = sample_instance.lookup_field(*field_numbers_opt);
        if (!field_opt) {
            throw std::invalid_argument("Field not found: " + field_path);
        }

        const auto& field = *field_opt;
        auto field_numbers = *field_numbers_opt; // Copy for lambda capture

        // Create accessor based on the field variant type
        return std::visit(
          [&field_path,
           field_numbers](const auto& value) -> FieldAccessorInfo<T> {
              using ValueType = std::decay_t<decltype(value)>;
              return create_field_accessor<ValueType>(
                field_path, field_numbers);
          },
          field.value);
    }

    /**
     * Get all available field paths.
     * Note: For dynamic registries, this is not efficiently implementable,
     * so we return an empty vector.
     */
    std::vector<std::string> get_field_paths() const override {
        // For dynamic registries, we can't efficiently enumerate all possible
        // paths without traversing the entire schema, so we return empty
        return {};
    }

private:
    /**
     * Convert a field path into a path of field numbers.
     */
    std::optional<std::vector<int32_t>>
    convert_field_path_to_numbers(const std::string& field_path) const {
        // Parse the field path into components
        std::vector<std::string_view> path_components;
        std::string_view path_view = field_path;

        size_t start = 0;
        while (start < path_view.size()) {
            size_t dot_pos = path_view.find('.', start);
            if (dot_pos == std::string_view::npos) {
                path_components.push_back(path_view.substr(start));
                break;
            }
            path_components.push_back(path_view.substr(start, dot_pos - start));
            start = dot_pos + 1;
        }

        // Convert field path to field numbers using protobuf reflection
        T sample_instance;
        return sample_instance.convert_field_path_to_numbers(path_components);
    }

    /**
     * Check if a field type is supported for accessor creation.
     */
    bool is_field_type_supported(
      const serde::pb::field::value_t& field_value) const {
        return std::visit(
          [](const auto& value) -> bool {
              using ValueType = std::decay_t<decltype(value)>;
              return std::is_same_v<ValueType, bool>
                     || std::is_same_v<ValueType, int32_t>
                     || std::is_same_v<ValueType, int64_t>
                     || std::is_same_v<ValueType, uint32_t>
                     || std::is_same_v<ValueType, uint64_t>
                     || std::is_same_v<ValueType, serde::pb::raw_enum_value>
                     || std::is_same_v<ValueType, float>
                     || std::is_same_v<ValueType, double>
                     || std::is_same_v<ValueType, ss::sstring>
                     || std::is_same_v<ValueType, iobuf>
                     || std::is_same_v<ValueType, absl::Time>
                     || std::is_same_v<ValueType, absl::Duration>
                     || std::is_same_v<ValueType, std::monostate>;
          },
          field_value);
    }

    /**
     * Create a field accessor for a specific value type.
     */
    template<typename ValueType>
    static FieldAccessorInfo<T> create_field_accessor(
      const std::string& field_path,
      const std::vector<int32_t>& field_numbers) {
        if constexpr (std::is_same_v<ValueType, bool>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::Bool,
              nullptr,
              nullptr,
              [field_numbers](const T& obj) -> bool {
                  return extract_field_value<bool>(obj, field_numbers);
              },
              nullptr,
              nullptr,
              nullptr};
        } else if constexpr (
          std::is_same_v<ValueType, int32_t>
          || std::is_same_v<ValueType, int64_t>
          || std::is_same_v<ValueType, uint32_t>
          || std::is_same_v<ValueType, uint64_t>
          || std::is_same_v<ValueType, serde::pb::raw_enum_value>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::Int64,
              [field_numbers](const T& obj) -> int64_t {
                  return extract_field_value<int64_t>(obj, field_numbers);
              },
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              nullptr};
        } else if constexpr (
          std::is_same_v<ValueType, float>
          || std::is_same_v<ValueType, double>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::Double,
              nullptr,
              [field_numbers](const T& obj) -> double {
                  return extract_field_value<double>(obj, field_numbers);
              },
              nullptr,
              nullptr,
              nullptr,
              nullptr};
        } else if constexpr (
          std::is_same_v<ValueType, ss::sstring>
          || std::is_same_v<ValueType, iobuf>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::String,
              nullptr,
              nullptr,
              nullptr,
              [field_numbers](const T& obj) -> std::string {
                  return extract_field_value<std::string>(obj, field_numbers);
              },
              nullptr,
              nullptr};
        } else if constexpr (std::is_same_v<ValueType, absl::Time>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::Timestamp,
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              [field_numbers](const T& obj) -> absl::Time {
                  return extract_field_value<absl::Time>(obj, field_numbers);
              }};
        } else if constexpr (std::is_same_v<ValueType, absl::Duration>) {
            return FieldAccessorInfo<T>{
              FieldAccessorInfo<T>::Duration,
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              [field_numbers](const T& obj) -> absl::Duration {
                  return extract_field_value<absl::Duration>(
                    obj, field_numbers);
              },
              nullptr};
        } else {
            throw std::invalid_argument(
              "Unsupported field type for filtering: " + field_path);
        }
    }

    /**
     * Extract a field value from an object using field numbers.
     */
    template<typename ReturnType>
    static ReturnType extract_field_value(
      const T& obj, const std::vector<int32_t>& field_numbers) {
        // TODO: revisit this
        T& mutable_obj = const_cast<T&>(obj); // Safe for lookup operations

        // TODO: update const-ness of codegen
        auto field_numbers_span = std::span<int32_t>{
          const_cast<int32_t*>(field_numbers.data()), field_numbers.size()};

        auto field_opt = mutable_obj.lookup_field(field_numbers_span);
        if (!field_opt) {
            throw std::runtime_error("Field lookup failed during extraction");
        }

        const auto& field = *field_opt;

        return std::visit(
          [](const auto& value) -> ReturnType {
              using ValueType = std::decay_t<decltype(value)>;

              if constexpr (
                std::is_same_v<ReturnType, bool>
                && std::is_same_v<ValueType, bool>) {
                  return value;
              } else if constexpr (std::is_same_v<ReturnType, int64_t>) {
                  if constexpr (std::is_same_v<ValueType, int32_t>) {
                      return static_cast<int64_t>(value);
                  } else if constexpr (std::is_same_v<ValueType, int64_t>) {
                      return value;
                  } else if constexpr (std::is_same_v<ValueType, uint32_t>) {
                      return static_cast<int64_t>(value);
                  } else if constexpr (std::is_same_v<ValueType, uint64_t>) {
                      return static_cast<int64_t>(value);
                  } else if constexpr (std::is_same_v<
                                         ValueType,
                                         serde::pb::raw_enum_value>) {
                      return static_cast<int64_t>(value.number);
                  } else {
                      throw std::runtime_error(
                        "Cannot convert field value to int64");
                  }
              } else if constexpr (std::is_same_v<ReturnType, double>) {
                  if constexpr (std::is_same_v<ValueType, float>) {
                      return static_cast<double>(value);
                  } else if constexpr (std::is_same_v<ValueType, double>) {
                      return value;
                  } else {
                      throw std::runtime_error(
                        "Cannot convert field value to double");
                  }
              } else if constexpr (std::is_same_v<ReturnType, std::string>) {
                  if constexpr (std::is_same_v<ValueType, ss::sstring>) {
                      return std::string(value);
                  } else if constexpr (std::is_same_v<ValueType, iobuf>) {
                      // Convert iobuf to string - need to find the correct
                      // method Based on redpanda codebase, iobuf likely has a
                      // linearize method or similar For now, we'll use a
                      // simpler approach or exclude iobuf fields from string
                      // conversion
                      throw std::runtime_error(
                        "iobuf to string conversion not implemented - use "
                        "manual field registration");
                  } else {
                      throw std::runtime_error(
                        "Cannot convert field value to string");
                  }
              } else if constexpr (
                std::is_same_v<ReturnType, absl::Time>
                && std::is_same_v<ValueType, absl::Time>) {
                  return value;
              } else if constexpr (
                std::is_same_v<ReturnType, absl::Duration>
                && std::is_same_v<ValueType, absl::Duration>) {
                  return value;
              } else if constexpr (std::is_same_v<ValueType, std::monostate>) {
                  // Handle unset optional fields
                  if constexpr (std::is_same_v<ReturnType, bool>) {
                      return false;
                  } else if constexpr (std::is_same_v<ReturnType, int64_t>) {
                      return 0;
                  } else if constexpr (std::is_same_v<ReturnType, double>) {
                      return 0.0;
                  } else if constexpr (std::
                                         is_same_v<ReturnType, std::string>) {
                      return std::string{};
                  } else if constexpr (std::is_same_v<ReturnType, absl::Time>) {
                      return absl::UnixEpoch();
                  } else if constexpr (std::is_same_v<
                                         ReturnType,
                                         absl::Duration>) {
                      return absl::ZeroDuration();
                  } else {
                      throw std::runtime_error(
                        "Cannot extract value from unset field");
                  }
              } else {
                  throw std::runtime_error(
                    "Unsupported field value type conversion");
              }
          },
          field.value);
    }
};

/**
 * Legacy builder class for backward compatibility.
 * Now uses dynamic registry under the hood but maintains the old interface.
 * @deprecated Use AutoProtobufFieldRegistry directly instead.
 */
template<typename T>
class AutoProtobufFieldRegistryBuilder {
public:
    static_assert(
      std::is_base_of_v<serde::pb::base_message, T>,
      "Type must inherit from serde::pb::base_message");

    using Registry = std::unique_ptr<IProtobufFieldRegistry<T>>;

    /**
     * Create a registry from a list of field paths using protobuf reflection.
     * @deprecated This method is now a no-op since AutoProtobufFieldRegistry
     * supports all fields dynamically. The field_paths parameter is ignored.
     */
    [[deprecated(
      "Use AutoProtobufFieldRegistry directly - field list no longer needed")]]
    static Registry
    create_registry(const std::vector<std::string>& /* field_paths */) {
        return std::make_unique<AutoProtobufFieldRegistry<T>>();
    }
};

/**
 * Convenience function to create a dynamic registry.
 */
template<typename T>
std::unique_ptr<IProtobufFieldRegistry<T>> make_auto_field_registry() {
    return std::make_unique<AutoProtobufFieldRegistry<T>>();
}

/**
 * Convenience function to create a manual registry.
 */
template<typename T>
std::unique_ptr<IProtobufFieldRegistry<T>> make_manual_field_registry(
  typename ProtobufFieldRegistry<T>::FieldAccessorMap accessors) {
    return std::make_unique<ProtobufFieldRegistry<T>>(std::move(accessors));
}

} // namespace redpanda::admin

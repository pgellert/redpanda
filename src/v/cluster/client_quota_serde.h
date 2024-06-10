
// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "base/seastarx.h"
#include "seastar/core/sstring.hh"
#include "serde/envelope.h"
#include "serde/rw/variant.h"

#include <absl/container/flat_hash_set.h>

#include <cstdint>
#include <iosfwd>
#include <vector>

namespace cluster::client_quota {

/// entity_key is used to key client quotas. It consists of multiple parts as a
/// key can be a combination of key parts. Currently, only client id based and
/// Redpanda-specific client id prefix-based quotas are supported, so all entity
/// keys will consist of a single part, but in the future if we extend to client
/// id and user principal based quotas, the entity key can contain two parts.
struct entity_key
  : serde::envelope<entity_key, serde::version<0>, serde::compat_version<0>> {
    struct part
      : serde::envelope<part, serde::version<0>, serde::compat_version<0>> {
        friend bool operator==(const part&, const part&) = default;
        friend std::ostream& operator<<(std::ostream&, const part&);

        template<typename H>
        friend H AbslHashValue(H h, const part& e) {
            return H::combine(std::move(h), e.part);
        }

        /// client_id_default_match is the quota entity type corresponding to
        /// /config/clients/<default>
        struct client_id_default_match
          : serde::envelope<
              client_id_default_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool operator==(
              const client_id_default_match&, const client_id_default_match&)
              = default;

            friend std::ostream&
            operator<<(std::ostream&, const client_id_default_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_default_match&) {
                return H::combine(
                  std::move(h), typeid(client_id_default_match).hash_code());
            }
        };

        /// client_id_match is the quota entity type corresponding to
        /// /config/clients/<client-id>
        struct client_id_match
          : serde::envelope<
              client_id_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool
            operator==(const client_id_match&, const client_id_match&)
              = default;

            friend std::ostream&
            operator<<(std::ostream&, const client_id_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_match& c) {
                return H::combine(
                  std::move(h), typeid(client_id_match).hash_code(), c.value);
            }

            ss::sstring value;
        };

        /// client_id_prefix_match is the quota entity type corresponding to the
        /// Redpanda-specific client prefix match
        /// /config/client-id-prefix/<client-id-prefix>
        struct client_id_prefix_match
          : serde::envelope<
              client_id_prefix_match,
              serde::version<0>,
              serde::compat_version<0>> {
            friend bool operator==(
              const client_id_prefix_match&, const client_id_prefix_match&)
              = default;

            friend std::ostream&
            operator<<(std::ostream&, const client_id_prefix_match&);

            template<typename H>
            friend H AbslHashValue(H h, const client_id_prefix_match& c) {
                return H::combine(
                  std::move(h),
                  typeid(client_id_prefix_match).hash_code(),
                  c.value);
            }

            ss::sstring value;
        };

        serde::variant<
          client_id_default_match,
          client_id_match,
          client_id_prefix_match>
          part;
    };

    auto serde_fields() { return std::tie(parts); }

    friend bool operator==(const entity_key&, const entity_key&) = default;
    friend std::ostream& operator<<(std::ostream&, const entity_key&);

    template<typename H>
    friend H AbslHashValue(H h, const entity_key& e) {
        return AbslHashValue(std::move(h), e.parts);
    }

    absl::flat_hash_set<part> parts;
};

/// entity_value describes the quotas diff for an entity_key
struct entity_value
  : serde::envelope<entity_value, serde::version<0>, serde::compat_version<0>> {
    enum key : int8_t {
        producer_byte_rate = 0,
        consumer_byte_rate,
        controller_mutation_rate,
    };

    enum operation : int8_t {
        upsert = 0,
        remove,
    };

    struct entry
      : serde::
          envelope<entity_value, serde::version<0>, serde::compat_version<0>> {
        constexpr entry() noexcept = default;
        constexpr entry(operation op, key type, uint64_t value) noexcept
          : op(op)
          , type(type)
          , value(value) {}
        constexpr entry(key type, uint64_t value) noexcept
          : entry(operation::upsert, type, value) {}

        friend bool operator<=>(const entry&, const entry&) = default;
        friend std::ostream& operator<<(std::ostream&, const entry&);

        constexpr auto serde_fields() { return std::tie(op, type, value); }

        template<typename H>
        constexpr friend H AbslHashValue(H h, const entry& e) {
            switch (e.op) {
            case entity_value::operation::upsert:
                return H::combine(std::move(h), e.op, e.type, e.value);
            case entity_value::operation::remove:
                return H::combine(std::move(h), e.op, e.type);
            }
        }

        operation op{};
        key type{};
        uint64_t value{};
    };

    friend bool operator==(const entity_value&, const entity_value&) = default;
    friend std::ostream& operator<<(std::ostream&, const entity_value&);

    auto serde_fields() { return std::tie(entries); }

    bool is_empty() const { return entries.empty(); }

    absl::flat_hash_set<entry> entries;
};

constexpr std::string_view to_string_view(entity_value::key e) {
    switch (e) {
    case cluster::client_quota::entity_value::key::producer_byte_rate:
        return "producer_byte_rate";
    case cluster::client_quota::entity_value::key::consumer_byte_rate:
        return "consumer_byte_rate";
    case cluster::client_quota::entity_value::key::controller_mutation_rate:
        return "controller_mutation_rate";
    }
}

struct alter_delta_cmd_data
  : serde::envelope<
      alter_delta_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    struct upsert_op
      : serde::
          envelope<upsert_op, serde::version<0>, serde::compat_version<0>> {
        client_quota::entity_key key;
        client_quota::entity_value value;
        auto serde_fields() { return std::tie(key, value); }
    };

    struct remove_op
      : serde::
          envelope<remove_op, serde::version<0>, serde::compat_version<0>> {
        client_quota::entity_key key;
        auto serde_fields() { return std::tie(key); }
    };

    std::vector<upsert_op> upsert;
    std::vector<remove_op> remove;

    friend bool
    operator==(const alter_delta_cmd_data&, const alter_delta_cmd_data&)
      = default;
};

} // namespace cluster::client_quota

/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "container/chunked_vector.h"
#include "json/iobuf_writer.h"
#include "json/writer.h"
#include "kafka/protocol/errors.h"
#include "model/metadata.h"
#include "pandaproxy/schema_registry/error.h"
#include "strings/string_switch.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <__expected/expected.h>
#include <avro/ValidSchema.hh>

#include <algorithm>
#include <cctype>
#include <iosfwd>
#include <type_traits>

namespace pandaproxy::schema_registry {

using is_mutable = ss::bool_class<struct is_mutable_tag>;
using permanent_delete = ss::bool_class<struct delete_tag>;
using include_deleted = ss::bool_class<struct include_deleted_tag>;
using is_deleted = ss::bool_class<struct is_deleted_tag>;
using default_to_global = ss::bool_class<struct default_to_global_tag>;
using force = ss::bool_class<struct force_tag>;
using normalize = ss::bool_class<struct normalize_tag>;
using verbose = ss::bool_class<struct verbose_tag>;

template<typename E>
std::enable_if_t<std::is_enum_v<E>, std::optional<E>>
  from_string_view(std::string_view);

enum class mode { import = 0, read_only, read_write };

constexpr std::string_view to_string_view(mode e) {
    switch (e) {
    case mode::import:
        return "IMPORT";
    case mode::read_only:
        return "READONLY";
    case mode::read_write:
        return "READWRITE";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<mode> from_string_view<mode>(std::string_view sv) {
    return string_switch<std::optional<mode>>(sv)
      .match(to_string_view(mode::import), mode::import)
      .match(to_string_view(mode::read_only), mode::read_only)
      .match(to_string_view(mode::read_write), mode::read_write)
      .default_match(std::nullopt);
}

enum class schema_type { avro = 0, json, protobuf };

constexpr std::string_view to_string_view(schema_type e) {
    switch (e) {
    case schema_type::avro:
        return "AVRO";
    case schema_type::json:
        return "JSON";
    case schema_type::protobuf:
        return "PROTOBUF";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<schema_type>
from_string_view<schema_type>(std::string_view sv) {
    return string_switch<std::optional<schema_type>>(sv)
      .match(to_string_view(schema_type::avro), schema_type::avro)
      .match(to_string_view(schema_type::json), schema_type::json)
      .match(to_string_view(schema_type::protobuf), schema_type::protobuf)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, const schema_type& v);

enum class output_format { none = 0, resolved, ignore_extensions, serialized };

constexpr std::string_view to_string_view(output_format of) {
    switch (of) {
    case output_format::resolved:
        return "resolved";
    case output_format::ignore_extensions:
        return "ignore_extensions";
    case output_format::serialized:
        return "serialized";
    case output_format::none:
        break;
    }
    return "";
}

template<>
inline std::optional<output_format>
from_string_view<output_format>(std::string_view sv) {
    return string_switch<std::optional<output_format>>(sv)
      .match(to_string_view(output_format::none), output_format::none)
      .match(to_string_view(output_format::resolved), output_format::resolved)
      .match(
        to_string_view(output_format::ignore_extensions),
        output_format::ignore_extensions)
      .match(
        to_string_view(output_format::serialized), output_format::serialized)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, const output_format& of);

///\brief Type representing a global resource for ACLs.
using registry_resource = named_type<ss::sstring, struct registry_resource_tag>;

///\brief A subject is the name under which a schema is registered.
///
/// Typically it will be "<topic>-key" or "<topic>-value".
using subject = named_type<ss::sstring, struct subject_tag>;

// An empty subject constant. In schema registry records, a context_subject with
// an empty subject field indicates a context-level operation (e.g., set the
// context to IMPORT mode).
static const subject empty_subject{""};

/// \brief A schema context, used for namespacing schemas and schema ids. Can be
/// used to implement multi-tenancy, environment (e.g., dev, staging, prod)
/// separation, and so on. By default, schemas are stored under the "." context.
using context = named_type<ss::sstring, struct context_tag>;
static const context default_context{"."};

// A subject that is valid within a context.
struct context_subject {
    context ctx;
    subject sub;

    // TODO: remove this, it is only for easier source code migration
    context_subject(subject s)
      : ctx{default_context}
      , sub{std::move(s)} {}

    constexpr context_subject() = default;

    context_subject(context c, subject s)
      : ctx{std::move(c)}
      , sub{std::move(s)} {}

    friend bool operator==(const context_subject&, const context_subject&)
      = default;

    friend auto
    operator<=>(const context_subject& lhs, const context_subject& rhs) {
        if (auto cmp = lhs.ctx() <=> rhs.ctx(); cmp != 0) {
            return cmp;
        }
        return lhs.sub() <=> rhs.sub();
    }

    template<typename H>
    friend H AbslHashValue(H h, const context_subject& key) {
        return H::combine(std::move(h), key.ctx, key.sub);
    }

    ss::sstring to_string() const {
        // Format as qualified subject: ":.context:subject" or "subject" if
        // default context
        if (ctx == pandaproxy::schema_registry::default_context) {
            return ss::format("{}", sub);
        }
        return ss::format(":{}:{}", ctx(), sub());
    }

    static std::expected<context_subject, ss::sstring>
    from_string(std::string_view input) {
        // Check for qualified syntax: starts with ":."
        if (input.starts_with(":.")) {
            // Find the second colon that separates context from subject
            auto second_colon = input.find(':', 2);

            if (second_colon == std::string_view::npos) {
                // Malformed: has ":." prefix but no closing ":"
                return std::unexpected(
                  ss::format(
                    "Invalid qualified subject syntax '{}': expected format "
                    "':.context:subject'",
                    input));
            }

            // Extract context (includes the leading ".")
            auto ctx_str = input.substr(1, second_colon - 1);

            // Validate context name
            if (ctx_str.empty() || ctx_str[0] != '.') {
                return std::unexpected(
                  ss::format(
                    "Invalid context name '{}': must start with '.'", ctx_str));
            }

            // Validate context characters (only after the initial ".")
            auto is_valid_context_char = [](char c) {
                // TODO: use absl for isalnum
                return std::isalnum(static_cast<unsigned char>(c)) || c == '.'
                       || c == '_' || c == '-';
            };

            if (!std::all_of(
                  ctx_str.begin() + 1, ctx_str.end(), is_valid_context_char)) {
                return std::unexpected(
                  ss::format(
                    "Invalid context name '{}': may only contain alphanumeric "
                    "characters, '.', '_', or '-'",
                    ctx_str));
            }

            // Extract subject name (everything after second colon)
            auto sub_str = input.substr(second_colon + 1);

            if (sub_str.empty()) {
                return std::unexpected(
                  ss::format(
                    "Invalid qualified subject '{}': subject name cannot be "
                    "empty",
                    input));
            }

            return context_subject{
              context{ss::sstring{ctx_str}}, subject{ss::sstring{sub_str}}};
        }

        // Unqualified subject - use default context
        if (input.empty()) {
            return std::unexpected(ss::sstring{"Subject name cannot be empty"});
        }

        return context_subject{default_context, subject{ss::sstring{input}}};
    }

    bool starts_with(const ss::sstring& prefix) const {
        // TODO: consider optimizing the starts_with check
        return to_string().starts_with(prefix);
    }

    // For authorizer support
    ss::sstring operator()() const { return ss::sstring{to_string()}; }
};

static const context_subject invalid_subject{default_context, subject{""}};

///\brief The version of the schema registered with a subject.
///
/// A subject may evolve its schema over time. Each version is associated with a
/// schema_id.
using schema_version = named_type<int32_t, struct schema_version_tag>;
static constexpr schema_version invalid_schema_version{-1};

struct schema_reference {
    friend bool
    operator==(const schema_reference& lhs, const schema_reference& rhs)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const schema_reference& ref);

    friend bool
    operator<(const schema_reference& lhs, const schema_reference& rhs);

    ss::sstring name;
    context_subject sub{invalid_subject};
    schema_version version{invalid_schema_version};
};

///\brief Definition of a schema and its type.
class schema_definition {
    using schema_definition_iobuf
      = named_type<iobuf, struct schema_definition_tag>;

public:
    struct raw_string : schema_definition_iobuf {
        raw_string() = default;
        explicit raw_string(iobuf&& buf) noexcept
          : schema_definition_iobuf{std::move(buf)} {}
        explicit raw_string(std::string_view sv)
          : schema_definition_iobuf{iobuf::from(sv)} {}
    };
    using references = chunked_vector<schema_reference>;

    schema_definition() = default;
    schema_definition(schema_definition&&) noexcept = default;
    schema_definition(const schema_definition&) = delete;
    schema_definition& operator=(schema_definition&&) noexcept = default;
    schema_definition& operator=(const schema_definition& other) = delete;
    ~schema_definition() noexcept = default;

    template<typename T>
    schema_definition(T&& def, schema_type type)
      : _def{std::forward<T>(def)}
      , _type{type}
      , _refs{} {}

    template<typename T>
    schema_definition(T&& def, schema_type type, references refs)
      : _def{std::forward<T>(def)}
      , _type{type}
      , _refs{std::move(refs)} {}

    friend bool
    operator==(const schema_definition& lhs, const schema_definition& rhs)
      = default;

    friend std::ostream& operator<<(std::ostream& os, const schema_definition&);

    schema_type type() const { return _type; }

    const raw_string& raw() const& { return _def; }
    raw_string raw() && { return std::move(_def); }
    raw_string shared_raw() const {
        auto& buf = const_cast<iobuf&>(_def());
        return raw_string{buf.share(0, buf.size_bytes())};
    }

    const references& refs() const& { return _refs; }
    references refs() && { return std::move(_refs); }

    schema_definition share() const {
        return {shared_raw(), type(), refs().copy()};
    }

    schema_definition copy() const {
        return {raw_string{_def().copy()}, type(), refs().copy()};
    }

    auto destructure() && {
        return std::make_tuple(std::move(_def), _type, std::move(_refs));
    }

private:
    raw_string _def;
    schema_type _type{schema_type::avro};
    references _refs;
};

///\brief The definition of an avro schema.
class avro_schema_definition {
public:
    explicit avro_schema_definition(
      avro::ValidSchema vs, schema_definition::references refs);

    schema_definition::raw_string raw() const;
    const schema_definition::references& refs() const { return _refs; };

    const avro::ValidSchema& operator()() const;

    friend bool operator==(
      const avro_schema_definition& lhs, const avro_schema_definition& rhs);

    friend std::ostream&
    operator<<(std::ostream& os, const avro_schema_definition& rhs);

    constexpr schema_type type() const { return schema_type::avro; }

    explicit operator schema_definition() const {
        return {raw(), type(), refs().copy()};
    }

    ss::sstring name() const;

private:
    avro::ValidSchema _impl;
    schema_definition::references _refs;
};

class protobuf_schema_definition {
public:
    struct impl;
    using pimpl = ss::shared_ptr<const impl>;

    explicit protobuf_schema_definition(
      pimpl p, schema_definition::references refs)
      : _impl{std::move(p)}
      , _refs(std::move(refs)) {}

    schema_definition::raw_string
    raw(output_format format = output_format::none) const;
    const schema_definition::references& refs() const { return _refs; };

    const impl& operator()() const { return *_impl; }

    friend bool operator==(
      const protobuf_schema_definition& lhs,
      const protobuf_schema_definition& rhs);

    friend std::ostream&
    operator<<(std::ostream& os, const protobuf_schema_definition& rhs);

    constexpr schema_type type() const { return schema_type::protobuf; }

    protobuf_schema_definition copy() const {
        return protobuf_schema_definition{_impl, _refs.copy()};
    }

    ::result<ss::sstring, kafka::error_code>
    name(const std::vector<int>& fields) const;

private:
    pimpl _impl;
    schema_definition::references _refs;
};

class json_schema_definition {
public:
    struct impl;
    using pimpl = ss::shared_ptr<const impl>;

    explicit json_schema_definition(pimpl p)
      : _impl{std::move(p)} {}

    schema_definition::raw_string raw() const;
    const schema_definition::references& refs() const;

    const impl& operator()() const { return *_impl; }

    friend bool operator==(
      const json_schema_definition& lhs, const json_schema_definition& rhs);

    friend std::ostream&
    operator<<(std::ostream& os, const json_schema_definition& rhs);

    constexpr schema_type type() const { return schema_type::json; }

    explicit operator schema_definition() const {
        return {raw(), type(), refs().copy()};
    }

    ss::sstring name() const;

    // retrieve "title" property from the schema, used to form the record name
    std::optional<ss::sstring> title() const;

private:
    pimpl _impl;
};

///\brief A schema that has been validated.
class valid_schema {
    using impl = std::variant<
      avro_schema_definition,
      protobuf_schema_definition,
      json_schema_definition>;

    template<typename T>
    using disable_if_valid_schema = std::
      enable_if_t<!std::is_same_v<std::remove_cvref_t<T>, valid_schema>, int>;

    template<typename T>
    using enable_if_can_construct_impl = std::
      enable_if_t<std::is_constructible_v<impl, std::remove_cvref_t<T>>, int>;

public:
    ///\brief Converting constructor from variant types
    template<
      typename T,
      disable_if_valid_schema<T> = 0,
      enable_if_can_construct_impl<T> = 0>
    valid_schema(T&& def)
      : _impl{std::forward<T>(def)} {}

    template<typename V, typename... Args>
    decltype(auto) visit(V&& v, Args... args) const& {
        return std::visit(
          std::forward<V>(v), _impl, std::forward<Args>(args)...);
    }

    template<typename V, typename... Args>
    decltype(auto) visit(V&& v, Args... args) && {
        return std::visit(
          std::forward<V>(v), std::move(_impl), std::forward<Args>(args)...);
    }

    schema_type type() const {
        return visit([](const auto& def) { return def.type(); });
    }

    schema_definition::raw_string raw() const& {
        return visit([](auto&& def) {
            return schema_definition::raw_string{def.raw()()};
        });
    }

    schema_definition::raw_string raw() && {
        return visit([](auto& def) {
            return schema_definition::raw_string{std::move(def).raw()()};
        });
    }

    friend std::ostream& operator<<(std::ostream& os, const valid_schema& def) {
        def.visit([&os](const auto& def) { os << def; });
        return os;
    }

private:
    impl _impl;
};

///\brief Globally unique identifier for a schema.
using schema_id = named_type<int32_t, struct schema_id_tag>;
static constexpr schema_id invalid_schema_id{-1};

// A schema id that is valid within a context.
struct context_schema_id {
    context ctx;
    schema_id id;

    // TODO: remove this, it is only for easier source code migration
    context_schema_id(schema_id id)
      : ctx{default_context}
      , id{id} {}

    context_schema_id(context c, schema_id s)
      : ctx{std::move(c)}
      , id{s} {}

    friend bool operator==(const context_schema_id&, const context_schema_id&)
      = default;

    friend auto
    operator<=>(const context_schema_id& lhs, const context_schema_id& rhs) {
        if (auto cmp = lhs.ctx() <=> rhs.ctx(); cmp != 0) {
            return cmp;
        }
        return lhs.id() <=> rhs.id();
    }
};

struct subject_version {
    subject_version(context_subject s, schema_version v)
      : sub{std::move(s)}
      , version{v} {}
    context_subject sub;
    schema_version version;
};

// Very similar to topic_key_type, separate to avoid intermingling storage code
enum class seq_marker_key_type {
    invalid = 0,
    schema,
    delete_subject,
    config,
    mode
};

constexpr std::string_view to_string_view(seq_marker_key_type v) {
    switch (v) {
    case seq_marker_key_type::schema:
        return "schema";
    case seq_marker_key_type::delete_subject:
        return "delete_subject";
    case seq_marker_key_type::config:
        return "config";
    case seq_marker_key_type::mode:
        return "mode";
    case seq_marker_key_type::invalid:
        break;
    }
    return "invalid";
}

// Record the sequence+node where updates were made to a subject,
// in order to later generate tombstone keys when doing a permanent
// deletion.
struct seq_marker {
    std::optional<model::offset> seq;
    std::optional<model::node_id> node;
    schema_version version;
    seq_marker_key_type key_type{seq_marker_key_type::invalid};

    // Note that matching nullopts is possible on the seq and node fields.
    // This is intentional; both fields are particular to redpanda, so making
    // them optional provides compatibility with non-rp schema registries. If
    // either is not present, we can assume a collision has not occurred.
    friend bool operator==(const seq_marker&, const seq_marker&) = default;
    friend std::ostream& operator<<(std::ostream& os, const seq_marker& v);
};

///\brief A schema with its subject
class subject_schema {
public:
    subject_schema() = default;

    subject_schema(context_subject sub, schema_definition def)
      : _sub{std::move(sub)}
      , _def{std::move(def)} {}

    friend bool operator==(const subject_schema& lhs, const subject_schema& rhs)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const subject_schema& schema);

    const context_subject& sub() const& { return _sub; }
    context_subject sub() && { return std::move(_sub); }

    schema_type type() const { return _def.type(); }

    const schema_definition& def() const& { return _def; }
    schema_definition def() && { return std::move(_def); }

    subject_schema share() const { return {sub(), def().share()}; }
    subject_schema copy() const { return {sub(), def().copy()}; }

    auto destructure() && {
        return std::make_tuple(std::move(_sub), std::move(_def));
    }

private:
    context_subject _sub{invalid_subject};
    schema_definition _def{"", schema_type::avro};
};

///\brief Complete description of a subject and schema for a version, as stored
/// in store
struct stored_schema {
    subject_schema schema;
    schema_version version{invalid_schema_version};
    schema_id id{invalid_schema_id};
    is_deleted deleted{false};
    stored_schema share() const {
        return {schema.share(), version, id, deleted};
    }
};

///\brief A mapping of version and schema id for a subject.
struct subject_version_entry {
    subject_version_entry(
      schema_version version, schema_id id, is_deleted deleted)
      : version{version}
      , id{id}
      , deleted(deleted) {}

    schema_version version;
    schema_id id;
    is_deleted deleted{is_deleted::no};
};

enum class compatibility_level {
    none = 0,
    backward,
    backward_transitive,
    forward,
    forward_transitive,
    full,
    full_transitive,
};

constexpr std::string_view to_string_view(compatibility_level v) {
    switch (v) {
    case compatibility_level::none:
        return "NONE";
    case compatibility_level::backward:
        return "BACKWARD";
    case compatibility_level::backward_transitive:
        return "BACKWARD_TRANSITIVE";
    case compatibility_level::forward:
        return "FORWARD";
    case compatibility_level::forward_transitive:
        return "FORWARD_TRANSITIVE";
    case compatibility_level::full:
        return "FULL";
    case compatibility_level::full_transitive:
        return "FULL_TRANSITIVE";
    }
    return "{invalid}";
}
template<>
constexpr std::optional<compatibility_level>
from_string_view<compatibility_level>(std::string_view sv) {
    return string_switch<std::optional<compatibility_level>>(sv)
      .match(
        to_string_view(compatibility_level::none), compatibility_level::none)
      .match(
        to_string_view(compatibility_level::backward),
        compatibility_level::backward)
      .match(
        to_string_view(compatibility_level::backward_transitive),
        compatibility_level::backward_transitive)
      .match(
        to_string_view(compatibility_level::forward),
        compatibility_level::forward)
      .match(
        to_string_view(compatibility_level::forward_transitive),
        compatibility_level::forward_transitive)
      .match(
        to_string_view(compatibility_level::full), compatibility_level::full)
      .match(
        to_string_view(compatibility_level::full_transitive),
        compatibility_level::full_transitive)
      .default_match(std::nullopt);
}

struct compatibility_result {
    friend bool
    operator==(const compatibility_result&, const compatibility_result&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const compatibility_result&);

    bool is_compat;
    chunked_vector<ss::sstring> messages;
};

} // namespace pandaproxy::schema_registry

template<>
struct fmt::formatter<pandaproxy::schema_registry::schema_reference> {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        auto it = ctx.begin();
        auto end = ctx.end();
        if (it != end && (*it == 'l' || *it == 'e')) {
            presentation = *it++;
        }
        if (it != end && *it != '}') {
            throw fmt::format_error("invalid format");
        }
        return it;
    }

    template<typename FormatContext>
    auto format(
      const pandaproxy::schema_registry::schema_reference& s,
      FormatContext& ctx) const -> decltype(ctx.out()) {
        if (presentation == 'l') {
            return fmt::format_to(
              ctx.out(),
              "name: {}, subject: {}, version: {}",
              s.name,
              s.sub,
              s.version);
        } else {
            return fmt::format_to(
              ctx.out(),
              "name='{}', subject='{}', version={}",
              s.name,
              s.sub,
              s.version);
        }
    }

    // l : format for logging
    // e : format for error_reporting
    char presentation{'l'};
};

template<>
struct fmt::formatter<pandaproxy::schema_registry::context_subject> {
    constexpr auto parse(fmt::format_parse_context& ctx)
      -> decltype(ctx.begin()) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(
      const pandaproxy::schema_registry::context_subject& key,
      FormatContext& ctx) const -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{}", key.to_string());
    }
};

namespace json {

template<typename Buffer>
void rjson_serialize(
  json::iobuf_writer<Buffer>& w,
  const pandaproxy::schema_registry::schema_definition::raw_string& def) {
    w.String(def());
}

template<typename Buffer>
void rjson_serialize(
  json::Writer<Buffer>& w,
  const pandaproxy::schema_registry::context_subject& ctx_sub) {
    w.String(ctx_sub.to_string());
}

} // namespace json

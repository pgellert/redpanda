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

#include "base/vlog.h"
#include "serde/rw/rw.h"

#include <array>
#include <type_traits>
#include <utility>
#include <variant>

namespace serde {

/// Mode tags for variant serialization compatibility.

/// strict_mode: Requires exact size match during deserialization.
/// Use when the set of alternatives is fixed and will never change.
struct strict_mode {
    static constexpr bool check_size = true;
};

/// compat_mode: Allows append-only evolution of alternatives.
///
/// IMPORTANT CONSTRAINTS:
/// 1. New alternatives may ONLY be appended at the end of the type list.
///    Never reorder, remove, or insert alternatives in the middle.
///
/// 2. Forward compatibility is LIMITED: if a newer writer serializes an
///    alternative unknown to an older reader, deserialization will throw.
///    Use feature flags/barriers to gate new alternatives until all
///    readers are upgraded.
///
/// 3. Backward compatibility works: newer readers can deserialize data
///    written by older writers (with fewer alternatives).
///
/// Example safe evolution:
///   v1: compat_variant<A, B>
///   v2: compat_variant<A, B, C>  // OK: C appended
///
/// Example UNSAFE evolution:
///   v1: compat_variant<A, B>
///   v2: compat_variant<A, C, B>  // WRONG: reordered
///   v2: compat_variant<A>        // WRONG: removed B
struct compat_mode {
    static constexpr bool check_size = false;
};

// A small wrapper around std::variant that is marked to be serializable.
//
// Special precation needs to be taken to mark a variant type as serializable
// with respect to compatibility. `serde::variant` should be a drop in
// replacement for `std::variant`, but allows for serde operations.
//
// The Mode template parameter controls compatibility behavior:
// - strict_mode (default): requires exact size match, use for fixed variants
// - compat_mode: ignores size, allows append-only evolution of alternatives
//
// # Variant Wire Compatibility:
//
// Variant is treated as a primitive atomic type, that means that *any* changes
// to the variant itself is not backwards compatible. `serde::variant` should
// always be wrapped in another `serde::envelope` to allow for changing the
// variant, and that wrapper struct needs to handle changes to the variant.
//
// Alternatively, use `serde::compat_variant` which allows adding new
// alternatives at the end without breaking compatibility.
template<typename Mode, typename... Types>
struct basic_variant : public std::variant<Types...> {
    using variant_type = std::variant<Types...>;
    using mode = Mode;

    constexpr basic_variant() noexcept(
      std::is_nothrow_default_constructible_v<
        std::variant_alternative_t<0, variant_type>>)
      = default;
    constexpr basic_variant(const basic_variant&) noexcept(
      std::is_nothrow_copy_constructible_v<variant_type>)
      = default;
    constexpr basic_variant(basic_variant&&) noexcept(
      std::is_nothrow_move_constructible_v<variant_type>)
      = default;

    // Ensure that this is not implicitly convertable from std::variant
    // but allow assignment from each individual type. For example:
    //
    // ```cpp
    // using my_variant = serde::variant<int, bool>
    //
    // my_variant v = false; // should compile
    //
    // my_variant v = std::variant<int, bool>(false); // should NOT compile
    // ```
    template<class T>
    constexpr basic_variant(T&& t) // NOLINT(*-explicit-*)
      noexcept(std::is_nothrow_constructible_v<variant_type, decltype(t)>)
    requires(
      !std::is_same_v<std::decay_t<T>, variant_type>
      && !std::is_same_v<std::decay_t<T>, basic_variant>
      && std::is_constructible_v<variant_type, T>)
      : variant_type(std::forward<T>(t)){};
    // Allow explicit conversion from std::variant
    explicit constexpr basic_variant(variant_type v) noexcept(
      std::is_nothrow_move_constructible_v<variant_type>)
      : variant_type(std::move(v)) {};

    template<class T, class... Args>
    constexpr explicit basic_variant(
      std::in_place_type_t<T> in_place,
      Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
      : variant_type(in_place, std::forward<Args...>(args)...) {}
    template<std::size_t I, class... Args>
    constexpr explicit basic_variant(
      std::in_place_index_t<I> in_place,
      Args&&... args) noexcept(std::
                                 is_nothrow_constructible_v<
                                   std::variant_alternative_t<I, variant_type>,
                                   Args...>)
      : variant_type(in_place, std::forward<Args...>(args)...) {}

    basic_variant& operator=(const basic_variant&) noexcept(
      std::is_nothrow_copy_assignable_v<variant_type>)
      = default;
    basic_variant& operator=(basic_variant&&) noexcept(
      std::is_nothrow_move_assignable_v<variant_type>)
      = default;

    constexpr ~basic_variant() noexcept = default;

    using variant_type::emplace;
    using variant_type::index;
    using variant_type::swap;
    using variant_type::valueless_by_exception;
};

template<typename Mode, typename... T>
void tag_invoke(tag_t<write_tag>, iobuf& out, basic_variant<Mode, T...> v) {
    write<size_t>(
      out,
      std::variant_size_v<typename std::decay_t<decltype(v)>::variant_type>);
    write<size_t>(out, v.index());
    std::visit(
      [&out]<typename V>(V&& val) { write(out, std::forward<V>(val)); },
      std::move(v));
}

namespace detail {

template<typename Variant>
struct variant_factory {
    using constructor = Variant (*)(iobuf_parser&, std::size_t);
    using constructor_table
      = std::array<constructor, std::variant_size_v<Variant>>;

    consteval variant_factory()
      : constructors([]<std::size_t... Index>(std::index_sequence<Index...>) {
          return std::to_array<constructor>({
            [](iobuf_parser& in, std::size_t bytes_left_limit) {
                return Variant{
                  std::in_place_index<Index>,
                  read_nested<std::variant_alternative_t<Index, Variant>>(
                    in, bytes_left_limit)};
            }...,
          });
      }(std::make_index_sequence<std::variant_size_v<Variant>>())) {}

    constructor_table constructors;
};

} // namespace detail

template<typename Mode, typename... T>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  basic_variant<Mode, T...>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    using UnderlyingType = typename Type::variant_type;

    auto size = read_nested<size_t>(in, bytes_left_limit);
    auto index = read_nested<size_t>(in, bytes_left_limit);

    if constexpr (Mode::check_size) {
        if (size != std::variant_size_v<UnderlyingType>) [[unlikely]] {
            throw serde_exception(fmt_with_ctx(
              ssx::sformat,
              "reading type {} of size {}: {} bytes left - unexpected variant "
              "size: {}, current variant size: {}, likely backwards compat "
              "issues.",
              type_str<Type>(),
              sizeof(Type),
              in.bytes_left(),
              size,
              std::variant_size_v<UnderlyingType>));
        }
    }
    if (index >= std::variant_size_v<UnderlyingType>) [[unlikely]] {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "reading type {} of size {}: {} bytes left - unexpected variant "
          "index: {}, variant size: {}",
          type_str<Type>(),
          sizeof(Type),
          in.bytes_left(),
          index,
          std::variant_size_v<UnderlyingType>));
    }
    constexpr detail::variant_factory<UnderlyingType> factory{};
    t = Type(factory.constructors[index](in, bytes_left_limit));
}

template<typename... Types>
using variant = basic_variant<strict_mode, Types...>;

template<typename... Types>
using compat_variant = basic_variant<compat_mode, Types...>;

} // namespace serde

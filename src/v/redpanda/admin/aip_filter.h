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

#include <algorithm>
#include <cctype>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

// Enum for comparison operators
enum class Op { EQ, NE, LT, GT, LE, GE };

// Abstract base class for any AST node (comparison or logical combination)
template<typename T>
struct ASTNode {
    virtual ~ASTNode() = default;
    virtual bool evaluate(const T& obj) const noexcept = 0;
};

// AST node for a logical AND combination of two expressions
template<typename T>
struct AndNode : public ASTNode<T> {
    std::unique_ptr<ASTNode<T>> left;
    std::unique_ptr<ASTNode<T>> right;

    AndNode(std::unique_ptr<ASTNode<T>> l, std::unique_ptr<ASTNode<T>> r)
      : left(std::move(l))
      , right(std::move(r)) {}

    bool evaluate(const T& obj) const noexcept override {
        return left->evaluate(obj) && right->evaluate(obj);
    }
};

// AST node for a field comparison (templated on the object type T and field
// value type F)
template<typename T, typename F>
struct ComparisonNode : public ASTNode<T> {
    std::function<F(const T&)> getField; // Extracts field value from object
    Op op;
    F literalValue; // The literal value to compare against

    ComparisonNode(std::function<F(const T&)> accessor, Op oper, F value)
      : getField(std::move(accessor))
      , op(oper)
      , literalValue(value) {}

    bool evaluate(const T& obj) const noexcept override {
        F fieldVal = getField(obj);
        switch (op) {
        case Op::EQ:
            return fieldVal == literalValue;
        case Op::NE:
            return fieldVal != literalValue;
        case Op::LT:
            return fieldVal < literalValue;
        case Op::GT:
            return fieldVal > literalValue;
        case Op::LE:
            return fieldVal <= literalValue;
        case Op::GE:
            return fieldVal >= literalValue;
        }
        return false; // unreachable
    }
};

template<typename T>
class Predicate {
public:
    Predicate(std::unique_ptr<ASTNode<T>> root)
      : _root(std::move(root)) {}

    // Evaluate the stored filter against an object (noexcept)
    bool operator()(const T& obj) const noexcept {
        if (!_root) {
            return true; // no filter means always match
        }
        return _root->evaluate(obj);
    }

private:
    std::unique_ptr<ASTNode<T>> _root;
};

// Field accessor registry for any type T
template<typename T>
struct FieldAccessorInfo {
    enum Type { Int64, Double, Bool, String } type;
    // We use std::function for each possible type (only one will be set, based
    // on 'type')
    std::function<int64_t(const T&)> getInt64;
    std::function<double(const T&)> getDouble;
    std::function<bool(const T&)> getBool;
    std::function<std::string(const T&)> getString;
};

template<typename T>
class FilterParser {
public:
    using FieldAccessorRegistry
      = std::unordered_map<std::string, FieldAccessorInfo<T>>;

    // Constructor takes a field accessor registry
    explicit FilterParser(const FieldAccessorRegistry& registry)
      : _registry(registry) {}

    // Parse the filter string into a Predicate object. Throws
    // std::invalid_argument on error.
    Predicate<T> parse(const std::string& filter) {
        Parser p(filter, _registry);
        std::unique_ptr<ASTNode<T>> root = p.parseExpression();
        p.skipSpaces();
        if (!p.endOfInput()) {
            throw std::invalid_argument(
              "Unexpected trailing characters in filter");
        }
        return Predicate<T>(std::move(root));
    }

private:
    const FieldAccessorRegistry& _registry;

    // Internal recursive descent parser
    class Parser {
    public:
        Parser(const std::string& input, const FieldAccessorRegistry& registry)
          : str(input)
          , pos(0)
          , _registry(registry) {}

        // Parse an expression: comparison { AND comparison }
        std::unique_ptr<ASTNode<T>> parseExpression() {
            auto leftNode = parseComparison();
            skipSpaces();
            // Handle multiple AND'ed conditions
            while (matchKeyword("AND")) { // case-insensitive match for "AND"
                skipSpaces();
                auto rightNode = parseComparison();
                // Combine the left and right nodes into an AndNode
                leftNode = std::make_unique<AndNode<T>>(
                  std::move(leftNode), std::move(rightNode));
                skipSpaces();
            }
            return leftNode;
        }

        // Parse a single comparison: FieldPath Op Literal
        std::unique_ptr<ASTNode<T>> parseComparison() {
            skipSpaces();
            std::string fieldPath = parseFieldPath();
            skipSpaces();
            Op op = parseOperator();
            skipSpaces();
            std::string literalText = parseLiteral();

            // Look up field in registry to get accessor and type
            auto it = _registry.find(fieldPath);
            if (it == _registry.end()) {
                throw std::invalid_argument("Unknown field path: " + fieldPath);
            }
            const FieldAccessorInfo<T>& info = it->second;

            // Based on field type, convert literal and create appropriate
            // ComparisonNode
            switch (info.type) {
            case FieldAccessorInfo<T>::Int64: {
                long long val = 0;
                try {
                    size_t idx = 0;
                    val = std::stoll(literalText, &idx);
                    if (idx != literalText.size()) {
                        throw std::invalid_argument("");
                    }
                } catch (...) {
                    throw std::invalid_argument(
                      "Expected integer value for field " + fieldPath);
                }
                return std::make_unique<ComparisonNode<T, int64_t>>(
                  info.getInt64, op, static_cast<int64_t>(val));
            }
            case FieldAccessorInfo<T>::Double: {
                double val = 0.0;
                try {
                    size_t idx = 0;
                    val = std::stod(literalText, &idx);
                    if (idx != literalText.size()) {
                        throw std::invalid_argument("");
                    }
                } catch (...) {
                    throw std::invalid_argument(
                      "Expected numeric value for field " + fieldPath);
                }
                return std::make_unique<ComparisonNode<T, double>>(
                  info.getDouble, op, val);
            }
            case FieldAccessorInfo<T>::Bool: {
                // Only allow = or != for bool comparisons
                if (op != Op::EQ && op != Op::NE) {
                    throw std::invalid_argument(
                      "Only '=' or '!=' supported for boolean field "
                      + fieldPath);
                }
                bool val;
                std::string litLower = literalText;
                std::transform(
                  litLower.begin(),
                  litLower.end(),
                  litLower.begin(),
                  ::tolower);
                if (litLower == "true") {
                    val = true;
                } else if (litLower == "false") {
                    val = false;
                } else {
                    throw std::invalid_argument(
                      "Expected boolean literal 'true' or 'false' for field "
                      + fieldPath);
                }
                return std::make_unique<ComparisonNode<T, bool>>(
                  info.getBool, op, val);
            }
            case FieldAccessorInfo<T>::String: {
                return std::make_unique<ComparisonNode<T, std::string>>(
                  info.getString, op, literalText);
            }
            }
            throw std::invalid_argument(
              "Unsupported field type in filter: " + fieldPath);
        }

        // Parse a field path (e.g., "field" or "nested.field").
        std::string parseFieldPath() {
            if (
              pos >= str.size()
              || !(std::isalpha(str[pos]) || str[pos] == '_')) {
                throw std::invalid_argument(
                  "Expected field name at position " + std::to_string(pos));
            }
            std::string field;
            // Parse identifier segments separated by '.'
            while (pos < str.size()) {
                char c = str[pos];
                if (std::isalnum(c) || c == '_') {
                    field.push_back(c);
                    pos++;
                } else if (c == '.') {
                    field.push_back(c);
                    pos++;
                    if (
                      pos >= str.size()
                      || !(std::isalpha(str[pos]) || str[pos] == '_')) {
                        throw std::invalid_argument(
                          "Expected field name after '.' at position "
                          + std::to_string(pos));
                    }
                    // continue parsing next identifier segment
                } else {
                    break;
                }
            }
            return field;
        }

        // Parse a comparison operator token
        Op parseOperator() {
            if (pos >= str.size()) {
                throw std::invalid_argument(
                  "Expected comparison operator at end of input");
            }
            char c = str[pos];
            if (c == '=') {
                pos++;
                return Op::EQ;
            }
            if (c == '!') {
                if (pos + 1 < str.size() && str[pos + 1] == '=') {
                    pos += 2;
                    return Op::NE;
                }
                throw std::invalid_argument(
                  "Unknown operator '!' at position " + std::to_string(pos));
            }
            if (c == '<') {
                if (pos + 1 < str.size() && str[pos + 1] == '=') {
                    pos += 2;
                    return Op::LE;
                } else {
                    pos++;
                    return Op::LT;
                }
            }
            if (c == '>') {
                if (pos + 1 < str.size() && str[pos + 1] == '=') {
                    pos += 2;
                    return Op::GE;
                } else {
                    pos++;
                    return Op::GT;
                }
            }
            throw std::invalid_argument(
              std::string("Expected comparison operator at position ")
              + std::to_string(pos));
        }

        // Parse a literal value (number, boolean, or quoted string).
        std::string parseLiteral() {
            if (pos >= str.size()) {
                throw std::invalid_argument(
                  "Expected literal value at end of input");
            }
            if (str[pos] == '\"') {
                // String literal - parse until closing quote
                pos++;
                std::string value;
                while (pos < str.size() && str[pos] != '\"') {
                    char c = str[pos++];
                    if (c == '\\' && pos < str.size()) {
                        // Handle escape sequences like \" or \\ if needed
                        char nextChar = str[pos++];
                        switch (nextChar) {
                        case '\"':
                            value.push_back('\"');
                            break;
                        case '\\':
                            value.push_back('\\');
                            break;
                        // ... (could handle \n, \t etc. if we want to support)
                        default:
                            value.push_back(nextChar);
                        }
                    } else {
                        value.push_back(c);
                    }
                }
                if (pos >= str.size() || str[pos] != '\"') {
                    throw std::invalid_argument(
                      "Unterminated string literal in filter");
                }
                pos++; // consume closing quote
                return value;
            } else {
                // Unquoted literal (could be numeric or boolean)
                size_t start = pos;
                while (pos < str.size()
                       && !std::isspace(static_cast<unsigned char>(str[pos]))) {
                    pos++;
                }
                std::string token = str.substr(start, pos - start);
                return token;
            }
        }

        // Skip whitespace characters
        void skipSpaces() {
            while (pos < str.size()
                   && std::isspace(static_cast<unsigned char>(str[pos]))) {
                pos++;
            }
        }

        // Match a keyword (like "AND"), case-insensitive. If matches, consume
        // it and return true.
        bool matchKeyword(const std::string& keyword) {
            skipSpaces();
            size_t len = keyword.size();
            if (pos + len <= str.size()) {
                // Compare ignoring case
                if (std::equal(
                      keyword.begin(),
                      keyword.end(),
                      str.begin() + pos,
                      [](char a, char b) {
                          return std::toupper(a) == std::toupper(b);
                      })) {
                    // Ensure the keyword is bounded by non-alphanumeric
                    if ((pos + len == str.size()
                         || std::isspace(
                           static_cast<unsigned char>(str[pos + len])))) {
                        pos += len;
                        return true;
                    }
                }
            }
            return false;
        }

        bool endOfInput() const { return pos >= str.size(); }

    private:
        const std::string& str;
        size_t pos;
        const FieldAccessorRegistry& _registry;
    };
};

// Helper function to create a field accessor registry builder
template<typename T>
class FieldAccessorRegistryBuilder {
public:
    using Registry = typename FilterParser<T>::FieldAccessorRegistry;

    FieldAccessorRegistryBuilder& addInt64Field(
      const std::string& fieldPath, std::function<int64_t(const T&)> accessor) {
        _registry[fieldPath] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Int64,
          std::move(accessor),
          nullptr,
          nullptr,
          nullptr};
        return *this;
    }

    FieldAccessorRegistryBuilder& addDoubleField(
      const std::string& fieldPath, std::function<double(const T&)> accessor) {
        _registry[fieldPath] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Double,
          nullptr,
          std::move(accessor),
          nullptr,
          nullptr};
        return *this;
    }

    FieldAccessorRegistryBuilder& addBoolField(
      const std::string& fieldPath, std::function<bool(const T&)> accessor) {
        _registry[fieldPath] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::Bool,
          nullptr,
          nullptr,
          std::move(accessor),
          nullptr};
        return *this;
    }

    FieldAccessorRegistryBuilder& addStringField(
      const std::string& fieldPath,
      std::function<std::string(const T&)> accessor) {
        _registry[fieldPath] = FieldAccessorInfo<T>{
          FieldAccessorInfo<T>::String,
          nullptr,
          nullptr,
          nullptr,
          std::move(accessor)};
        return *this;
    }

    Registry build() && { return std::move(_registry); }

private:
    Registry _registry;
};

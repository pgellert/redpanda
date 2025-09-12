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

#include "redpanda/admin/field_registry.h"

#include <algorithm>
#include <cctype>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

namespace redpanda::admin {

// Enum for comparison operators
enum class ComparisonOp { EQ, NE, LT, GT, LE, GE };

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

// AST node for a field comparison (templated on the field value type F)
template<typename T, typename F>
struct ComparisonNode : public ASTNode<T> {
    std::function<F(const T&)> getField; // Extracts field value from object
    ComparisonOp op;
    F literalValue; // The literal value to compare against

    ComparisonNode(
      std::function<F(const T&)> accessor, ComparisonOp oper, F value)
      : getField(std::move(accessor))
      , op(oper)
      , literalValue(value) {}

    bool evaluate(const T& obj) const noexcept override {
        F fieldVal = getField(obj);
        switch (op) {
        case ComparisonOp::EQ:
            return fieldVal == literalValue;
        case ComparisonOp::NE:
            return fieldVal != literalValue;
        case ComparisonOp::LT:
            return fieldVal < literalValue;
        case ComparisonOp::GT:
            return fieldVal > literalValue;
        case ComparisonOp::LE:
            return fieldVal <= literalValue;
        case ComparisonOp::GE:
            return fieldVal >= literalValue;
        }
        return false; // unreachable
    }
};

/**
 * A filter predicate that can be applied to objects of type T.
 */
template<typename T>
class FilterPredicate {
public:
    FilterPredicate(std::unique_ptr<ASTNode<T>> root)
      : root_(std::move(root)) {}

    // Evaluate the stored filter against an object (noexcept)
    bool operator()(const T& obj) const noexcept {
        if (!root_) {
            return true; // no filter means always match
        }
        return root_->evaluate(obj);
    }

private:
    std::unique_ptr<ASTNode<T>> root_;
};

/**
 * AIP (API Improvement Proposals) compliant filter parser for protobuf
 * messages.
 *
 * This class provides parsing of filter expressions according to Google's
 * AIP-160 filtering standard and creates filter predicates that can be applied
 * to objects.
 *
 * Supported syntax:
 * - Field comparisons: field = value, field != value, field < value, etc.
 * - Logical operators: AND (OR not yet supported)
 * - String literals: "quoted strings"
 * - Numeric literals: integers and floating point
 * - Boolean literals: true, false
 */
template<typename T>
class AIPFilterParser {
public:
    using Registry = ProtobufFieldRegistry<T>;

    /**
     * Construct an AIP filter parser with the given field registry.
     */
    explicit AIPFilterParser(const Registry& registry)
      : registry_(registry) {}

    /**
     * Parse a filter expression string into a callable predicate.
     *
     * @param filter_expression The filter expression string to parse
     * @return A callable predicate function that can be applied to objects of
     * type T
     * @throws std::invalid_argument if the filter expression is malformed or
     * references unknown fields
     */
    FilterPredicate<T> parse(const std::string& filter_expression) {
        if (filter_expression.empty()) {
            return FilterPredicate<T>(nullptr); // Empty filter matches all
        }

        Parser parser(filter_expression, registry_);
        std::unique_ptr<ASTNode<T>> root = parser.parseExpression();
        parser.skipSpaces();
        if (!parser.endOfInput()) {
            throw std::invalid_argument(
              "Unexpected trailing characters in filter");
        }
        return FilterPredicate<T>(std::move(root));
    }

    /**
     * Validate a filter expression without creating a predicate.
     */
    bool validate(const std::string& filter_expression) noexcept {
        try {
            parse(filter_expression);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

private:
    const Registry& registry_;

    // Internal recursive descent parser
    class Parser {
    public:
        Parser(const std::string& input, const Registry& registry)
          : str_(input)
          , pos_(0)
          , registry_(registry) {}

        // Parse an expression: comparison { AND comparison }
        std::unique_ptr<ASTNode<T>> parseExpression() {
            auto leftNode = parseComparison();
            skipSpaces();
            // Handle multiple AND'ed conditions
            while (matchKeyword("AND")) {
                skipSpaces();
                auto rightNode = parseComparison();
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
            ComparisonOp op = parseOperator();
            skipSpaces();
            std::string literalText = parseLiteral();

            const auto& info = registry_.get_field_info(fieldPath);

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
                if (op != ComparisonOp::EQ && op != ComparisonOp::NE) {
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
              pos_ >= str_.size()
              || !(std::isalpha(str_[pos_]) || str_[pos_] == '_')) {
                throw std::invalid_argument(
                  "Expected field name at position " + std::to_string(pos_));
            }
            std::string field;
            while (pos_ < str_.size()) {
                char c = str_[pos_];
                if (std::isalnum(c) || c == '_') {
                    field.push_back(c);
                    pos_++;
                } else if (c == '.') {
                    field.push_back(c);
                    pos_++;
                    if (
                      pos_ >= str_.size()
                      || !(std::isalpha(str_[pos_]) || str_[pos_] == '_')) {
                        throw std::invalid_argument(
                          "Expected field name after '.' at position "
                          + std::to_string(pos_));
                    }
                } else {
                    break;
                }
            }
            return field;
        }

        // Parse a comparison operator token
        ComparisonOp parseOperator() {
            if (pos_ >= str_.size()) {
                throw std::invalid_argument(
                  "Expected comparison operator at end of input");
            }
            char c = str_[pos_];
            if (c == '=') {
                pos_++;
                return ComparisonOp::EQ;
            }
            if (c == '!') {
                if (pos_ + 1 < str_.size() && str_[pos_ + 1] == '=') {
                    pos_ += 2;
                    return ComparisonOp::NE;
                }
                throw std::invalid_argument(
                  "Unknown operator '!' at position " + std::to_string(pos_));
            }
            if (c == '<') {
                if (pos_ + 1 < str_.size() && str_[pos_ + 1] == '=') {
                    pos_ += 2;
                    return ComparisonOp::LE;
                } else {
                    pos_++;
                    return ComparisonOp::LT;
                }
            }
            if (c == '>') {
                if (pos_ + 1 < str_.size() && str_[pos_ + 1] == '=') {
                    pos_ += 2;
                    return ComparisonOp::GE;
                } else {
                    pos_++;
                    return ComparisonOp::GT;
                }
            }
            throw std::invalid_argument(
              "Expected comparison operator at position "
              + std::to_string(pos_));
        }

        // Parse a literal value (number, boolean, or quoted string).
        std::string parseLiteral() {
            if (pos_ >= str_.size()) {
                throw std::invalid_argument(
                  "Expected literal value at end of input");
            }
            if (str_[pos_] == '\"') {
                pos_++;
                std::string value;
                while (pos_ < str_.size() && str_[pos_] != '\"') {
                    char c = str_[pos_++];
                    if (c == '\\' && pos_ < str_.size()) {
                        char nextChar = str_[pos_++];
                        switch (nextChar) {
                        case '\"':
                            value.push_back('\"');
                            break;
                        case '\\':
                            value.push_back('\\');
                            break;
                        default:
                            value.push_back(nextChar);
                        }
                    } else {
                        value.push_back(c);
                    }
                }
                if (pos_ >= str_.size() || str_[pos_] != '\"') {
                    throw std::invalid_argument(
                      "Unterminated string literal in filter");
                }
                pos_++;
                return value;
            } else {
                size_t start = pos_;
                while (
                  pos_ < str_.size()
                  && !std::isspace(static_cast<unsigned char>(str_[pos_]))) {
                    pos_++;
                }
                return str_.substr(start, pos_ - start);
            }
        }

        // Skip whitespace characters
        void skipSpaces() {
            while (pos_ < str_.size()
                   && std::isspace(static_cast<unsigned char>(str_[pos_]))) {
                pos_++;
            }
        }

        // Match a keyword (like "AND"), case-insensitive.
        bool matchKeyword(const std::string& keyword) {
            skipSpaces();
            size_t len = keyword.size();
            if (pos_ + len <= str_.size()) {
                if (std::equal(
                      keyword.begin(),
                      keyword.end(),
                      str_.begin() + pos_,
                      [](char a, char b) {
                          return std::toupper(a) == std::toupper(b);
                      })) {
                    if ((pos_ + len == str_.size()
                         || std::isspace(
                           static_cast<unsigned char>(str_[pos_ + len])))) {
                        pos_ += len;
                        return true;
                    }
                }
            }
            return false;
        }

        bool endOfInput() const { return pos_ >= str_.size(); }

    private:
        const std::string& str_;
        size_t pos_;
        const Registry& registry_;
    };
};

} // namespace redpanda::admin

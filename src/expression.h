#pragma once

#include "types.h"
#include <variant>
#include <memory>   // For std::unique_ptr
#include <stdexcept>

// Abstract base class for all expressions.
class Expression {
public:
    virtual ~Expression() = default;
    
    // Evaluates the expression against a given tuple and schema, returning the result.
    virtual Value evaluate(const Tuple& tuple, const Schema& schema) const = 0;
};

// --- LEAF EXPRESSIONS (the end points of the tree) ---

// Represents a constant, literal value (e.g., 100.0, "USA", true).
class ConstantExpression : public Expression {
public:
    explicit ConstantExpression(Value val) : value_(std::move(val)) {}
    
    // Evaluating a constant is easy: just return it.
    Value evaluate(const Tuple&, const Schema&) const override { return value_; }

private:
    Value value_;
};

// Represents a reference to a column (e.g., "c.balance").
class ColumnRefExpression : public Expression {
public:
    explicit ColumnRefExpression(std::string colName) : colName_(std::move(colName)) {}
    
    Value evaluate(const Tuple& tuple, const Schema& schema) const override {
        // Use the schema to find which index this column corresponds to...
        const auto& colInfo = schema.getColumn(colName_);
        // ...and return the value from that index in the tuple.
        return tuple[colInfo.index];
    }

    const std::string& getColumnName() const { return colName_; }
private:
    std::string colName_;
};


// --- INTERNAL EXPRESSIONS (they combine other expressions) ---

// Helper function to check if a Value is numeric (int or float)
inline bool is_numeric(const Value& v) {
    return std::holds_alternative<int>(v) || std::holds_alternative<float>(v);
}

// Helper function to convert any numeric Value to a double for calculations
inline double to_double(const Value& v) {
    if (std::holds_alternative<int>(v)) {
        return static_cast<double>(std::get<int>(v));
    }
    // No need to check for float, is_numeric already did.
    return static_cast<double>(std::get<float>(v));
}

class BinaryExpression : public Expression {
public:
    BinaryExpression(std::string op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
        : op_(std::move(op)), left_(std::move(left)), right_(std::move(right)) {}

    Value evaluate(const Tuple& tuple, const Schema& schema) const override {
    Value leftVal = left_->evaluate(tuple, schema);
    Value rightVal = right_->evaluate(tuple, schema);

    // --- Handle Arithmetic Operations ---
    if (op_ == "ADD" || op_ == "SUB" || op_ == "MUL" || op_ == "DIV") {
        if (!is_numeric(leftVal) || !is_numeric(rightVal)) {
            throw std::runtime_error("Arithmetic on non-numeric value is not allowed for operator: " + op_);
        }
        double left = to_double(leftVal);
        double right = to_double(rightVal);

        // THE FIX IS HERE: Cast the results back to float before returning
        if (op_ == "ADD") return static_cast<float>(left + right);
        if (op_ == "SUB") return static_cast<float>(left - right);
        if (op_ == "MUL") return static_cast<float>(left * right);
        if (op_ == "DIV") {
            if (right == 0.0) throw std::runtime_error("Division by zero.");
            return static_cast<float>(left / right);
        }
    }

    // --- Handle Comparison Operations ---
    if (op_ == "EQ") return leftVal == rightVal;
    if (op_ == "NEQ") return leftVal != rightVal;
    
    if (op_ == "GT" || op_ == "GTE" || op_ == "LT" || op_ == "LTE") {
        if (!is_numeric(leftVal) || !is_numeric(rightVal)) {
             throw std::runtime_error("Numeric comparison on non-numeric value for operator: " + op_);
        }
        double left = to_double(leftVal);
        double right = to_double(rightVal);

        if (op_ == "GT") return left > right;
        if (op_ == "GTE") return left >= right;
        if (op_ == "LT") return left < right;
        if (op_ == "LTE") return left <= right;
    }


        throw std::runtime_error("Unsupported or unimplemented binary operator: " + op_);
    }

private:
    std::string op_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

// Represents a unary NOT operation.
class NotExpression : public Expression {
public:
    explicit NotExpression(std::unique_ptr<Expression> expr) : expr_(std::move(expr)) {}
    
    Value evaluate(const Tuple& tuple, const Schema& schema) const override {
        Value val = expr_->evaluate(tuple, schema);
        return !std::get<bool>(val);
    }
private:
    std::unique_ptr<Expression> expr_;
};
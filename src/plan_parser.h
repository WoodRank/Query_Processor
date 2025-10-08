#pragma once

#include "operator.h"
#include "expression.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Forward declarations because parsePlan and parseExpression can call each other.
std::unique_ptr<Expression> parseExpression(const json& exprJson);
std::unique_ptr<Operator> parsePlan(const json& planJson, Catalog& catalog, const std::string& dataDir);

// Parses an expression object from the JSON plan.
inline std::unique_ptr<Expression> parseExpression(const json& exprJson) {
    if (exprJson.contains("const")) {
        const auto& type = exprJson["type"];
        if (type == "int") return std::make_unique<ConstantExpression>(exprJson["const"].get<int>());
        if (type == "float") return std::make_unique<ConstantExpression>(exprJson["const"].get<float>());
        if (type == "string") return std::make_unique<ConstantExpression>(exprJson["const"].get<std::string>());
        if (type == "bool") return std::make_unique<ConstantExpression>(exprJson["const"].get<bool>());
    }
    if (exprJson.contains("col")) {
        return std::make_unique<ColumnRefExpression>(exprJson["col"]);
    }
    if (exprJson.contains("op")) {
        std::string op = exprJson["op"];
        if (op == "NOT") {
            return std::make_unique<NotExpression>(parseExpression(exprJson["expr"]));
        }
        // If it's not NOT, it must be a binary expression for this project.
        auto left = parseExpression(exprJson["left"]);
        auto right = parseExpression(exprJson["right"]);
        return std::make_unique<BinaryExpression>(op, std::move(left), std::move(right));
    }
    throw std::runtime_error("Invalid expression JSON format");
}

// Recursively parses the query plan and builds the C++ operator tree.
inline std::unique_ptr<Operator> parsePlan(const json& planJson, Catalog& catalog, const std::string& dataDir) {
    std::string op = planJson["op"];

    if (op == "Scan") {
        std::string table = planJson["table"];
        std::string alias = planJson["as"];
        std::string tablePath = dataDir + "/" + table;
        return std::make_unique<ScanOperator>(tablePath, alias, catalog);
    }
    if (op == "Select") {
        auto input = parsePlan(planJson["input"], catalog, dataDir);
        auto predicate = parseExpression(planJson["predicate"]);
        return std::make_unique<SelectOperator>(std::move(input), std::move(predicate));
    }
    if (op == "Project") {
        auto input = parsePlan(planJson["input"], catalog, dataDir);
        std::vector<ProjectOperator::ProjExpr> projExprs;
        for (const auto& exprNode : planJson["exprs"]) {
            projExprs.push_back({exprNode["as"], parseExpression(exprNode["expr"])});
        }
        return std::make_unique<ProjectOperator>(std::move(input), std::move(projExprs));
    }
    if (op == "Join") {
        auto left = parsePlan(planJson["left"], catalog, dataDir);
        auto right = parsePlan(planJson["right"], catalog, dataDir);
        auto condition = parseExpression(planJson["condition"]);
        return std::make_unique<NestedLoopJoinOperator>(std::move(left), std::move(right), std::move(condition));
    }
    if (op == "Limit") {
        auto input = parsePlan(planJson["input"], catalog, dataDir);
        int limit = planJson["limit"];
        return std::make_unique<LimitOperator>(std::move(input), limit);
    }

    throw std::runtime_error("Unsupported operator in plan: " + op);
}
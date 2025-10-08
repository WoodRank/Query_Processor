#pragma once

#include "operator.h"
#include "expression.h"
#include <nlohmann/json.hpp>
#include <set> // Added for predicate pushdown helpers

using json = nlohmann::json;

// Forward declarations because parsePlan and parseExpression can call each other.
std::unique_ptr<Expression> parseExpression(const json& exprJson);
std::unique_ptr<Operator> parsePlan(const json& planJson, Catalog& catalog, const std::string& dataDir);

// --- HELPER FUNCTIONS FOR PREDICATE PUSHDOWN ---

// Helper to get all column names from a schema
inline std::set<std::string> getSchemaColumnNames(const Schema& schema) {
    std::set<std::string> names;
    for (const auto& col : schema.getColumns()) {
        names.insert(col.name);
    }
    return names;
}

// Helper to check if a set of predicate columns is a subset of a schema's columns
inline bool isSubsetOf(const std::set<std::string>& predicateCols, const std::set<std::string>& schemaCols) {
    if (predicateCols.empty()) return false; // Don't push down constant-only predicates
    for (const auto& col : predicateCols) {
        if (schemaCols.find(col) == schemaCols.end()) {
            return false;
        }
    }
    return true;
}


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
        // If it's not NOT, it must be a binary expression
        return std::make_unique<BinaryExpression>(
            op,
            parseExpression(exprJson["left"]),
            parseExpression(exprJson["right"])
        );
    }
    throw std::runtime_error("Invalid expression JSON");
}

// Parses the main query plan object to build the operator tree.
inline std::unique_ptr<Operator> parsePlan(const json& planJson, Catalog& catalog, const std::string& dataDir) {
    std::string op = planJson["op"];

    // A local lambda to handle parsing any kind of join.
    // This avoids code duplication since a join can be a top-level operator
    // or exist underneath a Select operator.
    auto parseJoin = [&](const json& joinJson) -> std::unique_ptr<Operator> {
        std::string method = "nested_loop"; // Default join method
        if (joinJson.contains("method")) {
            method = joinJson["method"];
        }

        if (method == "hash") {
            std::cout << "[Planner] Using Hash Join." << std::endl;
            const auto& condJson = joinJson["condition"];
            if (!condJson.contains("op") || condJson["op"] != "EQ") {
                throw std::runtime_error("Hash join only supports equality predicates.");
            }

            auto left = parsePlan(joinJson["left"], catalog, dataDir);
            auto right = parsePlan(joinJson["right"], catalog, dataDir);
            
            auto leftKey = parseExpression(condJson["left"]);
            auto rightKey = parseExpression(condJson["right"]);
            
            // Check which key belongs to which side
            auto leftSchemaCols = getSchemaColumnNames(left->getSchema());
            std::set<std::string> leftKeyCols, rightKeyCols;
            leftKey->collectColumnRefs(leftKeyCols);
            rightKey->collectColumnRefs(rightKeyCols);
            
            if (isSubsetOf(leftKeyCols, leftSchemaCols) && isSubsetOf(rightKeyCols, getSchemaColumnNames(right->getSchema()))) {
                 return std::make_unique<HashJoinOperator>(std::move(left), std::move(right), std::move(leftKey), std::move(rightKey));
            } else if (isSubsetOf(rightKeyCols, leftSchemaCols) && isSubsetOf(leftKeyCols, getSchemaColumnNames(right->getSchema()))) {
                 // The keys were swapped in the plan (e.g., r.key = l.key), so we swap them back for the operator.
                 return std::make_unique<HashJoinOperator>(std::move(left), std::move(right), std::move(rightKey), std::move(leftKey));
            } else {
                 throw std::runtime_error("Hash join predicate columns do not align with join inputs.");
            }
        }
        
        if (method == "block_nested_loop") {
            std::cout << "[Planner] Using Block Nested-Loop Join." << std::endl;
            auto left = parsePlan(joinJson["left"], catalog, dataDir);
            auto right = parsePlan(joinJson["right"], catalog, dataDir);
            auto condition = parseExpression(joinJson["condition"]);
            return std::make_unique<BlockNestedLoopJoinOperator>(std::move(left), std::move(right), std::move(condition));
        }
        
        // Default to original Nested-Loop Join
        std::cout << "[Planner] Using Nested-Loop Join." << std::endl;
        auto left = parsePlan(joinJson["left"], catalog, dataDir);
        auto right = parsePlan(joinJson["right"], catalog, dataDir);
        auto condition = parseExpression(joinJson["condition"]);
        return std::make_unique<NestedLoopJoinOperator>(std::move(left), std::move(right), std::move(condition));
    };

    if (op == "Scan") {
        std::string table = planJson["table"];
        std::string alias = planJson["as"];
        std::string tablePath = dataDir + "/" + table;
        return std::make_unique<ScanOperator>(tablePath, alias, catalog);
    }
    if (op == "Select") {
        const auto& inputJson = planJson["input"];
        // --- PREDICATE PUSHDOWN LOGIC ---
        // Check if the input is a join, which is our optimization opportunity.
        if (inputJson.contains("op") && inputJson["op"] == "Join") {
            auto predicate = parseExpression(planJson["predicate"]);
            std::set<std::string> predicateCols;
            predicate->collectColumnRefs(predicateCols);

            // Parse the join's children to inspect their schemas.
            auto left = parsePlan(inputJson["left"], catalog, dataDir);
            auto right = parsePlan(inputJson["right"], catalog, dataDir);
            
            auto leftSchemaCols = getSchemaColumnNames(left->getSchema());
            auto rightSchemaCols = getSchemaColumnNames(right->getSchema());

            bool pushToLeft = isSubsetOf(predicateCols, leftSchemaCols);
            bool pushToRight = isSubsetOf(predicateCols, rightSchemaCols);

            // Apply the optimization if the predicate only uses columns from the left.
            if (pushToLeft) {
                std::cout << "[Optimizer] Pushing predicate to LEFT side of join." << std::endl;
                auto newLeft = std::make_unique<SelectOperator>(std::move(left), std::move(predicate));
                // Re-assemble the join on top of the new, filtered input.
                // We must use our new parseJoin lambda here to respect the chosen join method.
                // We fake a json object to pass to the lambda.
                json newJoinJson = inputJson;
                // The "left" and "right" in the json are now irrelevant because we pass the operators directly,
                // but the "method" and "condition" are still needed.
                auto newRight = std::move(right);
                auto condition = parseExpression(inputJson["condition"]);

                // This part is a bit tricky: we must manually reconstruct the correct join type
                // since we already have the operator inputs.
                 std::string method = "nested_loop";
                 if (inputJson.contains("method")) method = inputJson["method"];
                 if (method == "block_nested_loop") {
                     return std::make_unique<BlockNestedLoopJoinOperator>(std::move(newLeft), std::move(newRight), std::move(condition));
                 }
                 // We can't easily reconstruct the hash join here without re-parsing keys,
                 // so we'll just fall back to BNLJ for pushed-down hash joins for simplicity.
                 // A more advanced system would handle this better.
                 return std::make_unique<NestedLoopJoinOperator>(std::move(newLeft), std::move(newRight), std::move(condition));

            } 
            // Apply the optimization if the predicate only uses columns from the right.
            if (pushToRight) {
                 std::cout << "[Optimizer] Pushing predicate to RIGHT side of join." << std::endl;
                 auto newRight = std::make_unique<SelectOperator>(std::move(right), std::move(predicate));
                 auto newLeft = std::move(left);
                 auto condition = parseExpression(inputJson["condition"]);
                 
                 std::string method = "nested_loop";
                 if (inputJson.contains("method")) method = inputJson["method"];
                 if (method == "block_nested_loop") {
                     return std::make_unique<BlockNestedLoopJoinOperator>(std::move(newLeft), std::move(newRight), std::move(condition));
                 }
                 return std::make_unique<NestedLoopJoinOperator>(std::move(newLeft), std::move(newRight), std::move(condition));
            }
            
            // If predicate uses columns from both sides, it can't be pushed.
            // Build the Select on top of the Join as originally planned.
            auto join = parseJoin(inputJson);
            return std::make_unique<SelectOperator>(std::move(join), parseExpression(planJson["predicate"]));
        }
        
        // --- Fallback for non-join inputs (original behavior) ---
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
        return parseJoin(planJson);
    }
    if (op == "Limit") {
        auto input = parsePlan(planJson["input"], catalog, dataDir);
        int limit = planJson["limit"];
        return std::make_unique<LimitOperator>(std::move(input), limit);
    }
    throw std::runtime_error("Unknown operator type: " + op);
}
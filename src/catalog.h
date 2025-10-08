#pragma once

#include "types.h"
#include "nlohmann/json.hpp" // FIX 1: Corrected the include path
#include <fstream>
#include <filesystem>
#include <stdexcept> // For std::runtime_error

using json = nlohmann::json;

inline DataType stringToType(const std::string& typeStr) {
    if (typeStr == "int") return DataType::INT;
    if (typeStr == "float") return DataType::FLOAT;
    if (typeStr == "string") return DataType::STRING;
    if (typeStr == "bool") return DataType::BOOL;
    throw std::runtime_error("Unknown data type: " + typeStr);
}

class Catalog {
public:
    // Scans a directory for .schema.json files
    void loadSchemas(const std::string& dataDir) {
        std::cout << "[Catalog] Scanning directory: '" << dataDir << "'" << std::endl;
        
        for (const auto& entry : std::filesystem::directory_iterator(dataDir)) {
            // This is the robust and correct way to check the extension.
            if (entry.path().extension() == ".json") {
                std::cout << "[Catalog] Found schema file: '" << entry.path().string() << "'" << std::endl;
                loadSchemaFromFile(entry.path().string());
            }
        }
    }

    // FIX 2: ADDED THIS MISSING PUBLIC FUNCTION
    // Looks up the schema for a given table name (e.g., "customers.csv").
    const Schema& getSchema(const std::string& tableName) const {
        return schemas_.at(tableName);
    }

private:
    void loadSchemaFromFile(const std::string& schemaPath) {
        std::ifstream f(schemaPath);
        if (!f.is_open()) {
             throw std::runtime_error("Could not open schema file: " + schemaPath);
        }
        json schemaJson = json::parse(f);

        std::string csvFile = schemaJson["file"];
        Schema schema;
        for (const auto& col : schemaJson["columns"]) {
            schema.addColumn(col["name"], stringToType(col["type"]));
        }
        schemas_[csvFile] = schema; // Use _ to denote member variable
        std::cout << "[Catalog] Storing schema for key: '" << csvFile << "'" << std::endl;
        std::cout << "Loaded schema for " << csvFile << std::endl;
    }

    std::unordered_map<std::string, Schema> schemas_; // Use _ to denote member variable
}; // FIX 3: Added the missing semicolon here
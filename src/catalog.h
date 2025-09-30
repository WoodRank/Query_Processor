#pragma once

#include "types.h"
#include <nlohmann/json.hpp> 
#include <fstream>
#include <filesystem>

/*
    Reads all the .schema.json files and keeps track of the schema for every table
*/


using json = nlohmann::json;

//helper function to convert type name (string) from JSON to our DataType enum

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
    void loadSchemas(const std::string& dataDir){
        for (const auto&entry : std::filesystem::directory_iterator(dataDir)){
            if (entry.path().string().ends_with(".schema.json")){
                loadSchemaFrom
            }
        }
    }

    private:
        void loadSchemaFromFile(const std::string& schemaPath){
            std::ifstream f(schemaPath);
            json schemaJson = json::parse(f);

            std::string csvFile = schemaJson["file"];
            Schema schema;
            for (const auto& col: schemaJson["columns"]){
                schema.addColumn(col["name"], stringToType(col["type"]));
            }
            schemas[csvFile] = schema;
            std::cout << "Loaded schema for " << csvFile << std::endl;
        }

        std::unordered_map<std::string, Schema> schemas;
};
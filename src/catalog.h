#pragma once

#include "types.h"
#include <nlohmann/json.hpp> 
#include <fstream>
#include <filesystem>


using json = nlohmann::json;

//helper function to convert type name (string) from JSON to our DataType enum

ineline DataType stringToType(const std::string& typeStr) {
    if (typeStr == "int") return DataType::INT;
    if (typeStr == "float") return DataType::FLOAT;
    if (typeStr == "string") return DataType::STRING;
    if (typeStr == "bool") return DataType::BOOL;

    throw std::runtime_error("Unknown data type: " + typeStr);
}
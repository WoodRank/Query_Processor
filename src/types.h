#pragma once // Prevent the file from being included multiple times

#include <string>
#include <vector>
#include <variant>
#include <unordered_map>
#include <iostream>

/*
    This file is basically for representing data in memory. This shows what a single value looks like as well as how rows and tables schema look as well.
*/

using Value = std::variant<int, float, std::string, bool>;

// A Tuple is just a list of Values
using Tuple = std::vector<Value>;

// Enum for readability
enum class DataType
{
    INT,
    FLOAT,
    STRING,
    BOOL
};

// A struct to hold all the metadata for a single column
struct ColumnInfo
{
    std::string name;
    DataType type;
    size_t index; // Column position in the tuple
};

class Schema
{
public:
    // Adds a new column to the schema
    void addColumn(const std::string &name, DataType type)
    {
        schemaColumns.push_back({name, type, schemaColumns.size()});
        // Map column name to index for quicker lookups
        columnIndex[name] = schemaColumns.back().index;
    }
    const ColumnInfo &getColumn(const std::string &name) const
    {
        // return the column at the index
        return schemaColumns.at(columnIndex.at(name));
    }
    // return all columns
    const std::vector<ColumnInfo> getColumns() const
    {
        return schemaColumns;
    }

    /*
        This is a static method because it's not tied to a specific object's state.
        It acts as a utility for the Schema class as a whole, taking two schemas as
        input and producing a third one as output without modifying the originals.
     */
    static Schema merge(const Schema &left, const Schema &right)
    {
        Schema newSchema = left;

        for (const auto &col : right.getColumns())
        {
            newSchema.addColumn(col.name, col.type);
        }
        return newSchema;
    }

private:
    std::vector<ColumnInfo> schemaColumns;
    std::unordered_map<std::string, size_t> columnIndex;
};

// ADD THIS TO THE END OF src/types.h

// Helper function to print a single Value variant.
inline void printValue(const Value& val) {
    // std::visit is a clean way to handle all types in a variant.
    std::visit([](auto&& arg) {
        std::cout << arg;
    }, val);
}

// Helper function to print a whole tuple using its schema to label the columns.
inline void printTuple(const Tuple& tuple, const Schema& schema) {
    const auto& cols = schema.getColumns();
    for (size_t i = 0; i < tuple.size(); ++i) {
        std::cout << cols[i].name << ": ";
        printValue(tuple[i]);
        if (i < tuple.size() - 1) {
            std::cout << " | ";
        }
    }
    std::cout << std::endl;
}
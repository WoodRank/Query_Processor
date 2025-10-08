#include "plan_parser.h" // This includes everything else we need.
#include <iostream>

int main(int argc, char* argv[]) {
    // 1. Check that the user provided the right command-line arguments.
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <path_to_plan.json> <path_to_data_directory>" << std::endl;
        return 1;
    }

    std::string plan_path = argv[1];
    std::string data_dir = argv[2];

    try {
        // 2. Load all schemas from the data directory into our catalog.
        Catalog catalog;
        std::cout << "[main::Debug] Created catalog at address: " << &catalog << std::endl;
        catalog.loadSchemas(data_dir);
        
        // 3. Read and parse the query plan JSON file.
        std::ifstream plan_file(plan_path);
        if (!plan_file.is_open()) {
            throw std::runtime_error("Could not open plan file: " + plan_path);
        }
        json plan_json = json::parse(plan_file);

        // 4. Build the operator tree from the JSON plan.
        std::cout << "\nBuilding query plan..." << std::endl;
        auto root_operator = parsePlan(plan_json, catalog, data_dir);

        // 5. Execute the query and print the results tuple by tuple.
        root_operator->open();

        Tuple tuple;
        int row_count = 0;
        std::cout << "\n--- Query Results ---\n";
        while (root_operator->next(tuple)) {
            printTuple(tuple, root_operator->getSchema());
            row_count++;
        }
        std::cout << "---------------------\n";
        std::cout << "Returned " << row_count << " rows.\n";

        // 6. Clean up all resources.
        root_operator->close();

    } catch (const std::exception& e) {
        // Catch any errors that might have happened during parsing or execution.
        std::cerr << "\nError during execution: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
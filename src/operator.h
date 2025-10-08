#pragma once

#include "types.h"
#include "catalog.h"
#include <fstream>
#include <sstream>
#include <memory> // For std::unique_ptr
#include "expression.h"

// The abstract base class for all operators. This defines the "contract".
class Operator {
public:
    virtual ~Operator() = default; // Virtual destructor is important for base classes!

    virtual void open() = 0;
    virtual bool next(Tuple& tuple) = 0;
    virtual void close() = 0;
    
    // A helper to get the schema of the data this operator produces.
    virtual const Schema& getSchema() const = 0;
};


// Scan is the first operator. It reads tuples from a CSV file.
class ScanOperator : public Operator {
public:
    ScanOperator(const std::string& tablePath, const std::string& alias, const Catalog& catalog)
        : tablePath_(tablePath), alias_(alias), catalog_(catalog) {

            std::cout << "[ScanOperator::Debug] My catalog reference points to address: " << &catalog_ << std::endl;
            catalog_.printAddress();
        
        // When a Scan is created, it must also create its output schema.
        // It qualifies each column name with its alias (e.g., "custkey" -> "c.custkey").
       // Create a path object to easily get just the filename.
        std::filesystem::path p(tablePath_);
        std::string tableName = p.filename().string();

        std::cout << "[Scan] Looking up schema for key: '" << tableName << "'" << std::endl;
        
        // Now, use the correct key (the filename) for the catalog lookup.
        const Schema& baseSchema = catalog_.getSchema(tableName); 
        // -----------------------

        for (const auto& col : baseSchema.getColumns()) {
            qualifiedSchema_.addColumn(alias_ + "." + col.name, col.type);
        }
    }

    void open() override {
        // Don't open if already open.
        if (fileStream_.is_open()) return;
        
        fileStream_.open(tablePath_);
        if (!fileStream_.is_open()) {
            throw std::runtime_error("Cannot open data file: " + tablePath_);
        }   
        
        // IMPORTANT: Skip the header row of the CSV file.
        std::string header;
        std::getline(fileStream_, header);
    }

    bool next(Tuple& tuple) override {
        std::string line;
        if (std::getline(fileStream_, line)) {
            tuple.clear();
            std::stringstream ss(line);
            std::string field;
            int colIdx = 0;
            const auto& cols = qualifiedSchema_.getColumns();

            while (std::getline(ss, field, ',')) {
                 if (colIdx >= cols.size()) break; // Handle trailing commas or malformed lines
                 const auto& colInfo = cols[colIdx++];
                 
                 // This is where we parse the string from the CSV into our C++ types.
                 try {
                     switch (colInfo.type) {
                         case DataType::INT:    tuple.emplace_back(std::stoi(field)); break;
                         case DataType::FLOAT:  tuple.emplace_back(std::stof(field)); break;
                         case DataType::STRING: tuple.emplace_back(field); break;
                         case DataType::BOOL:   tuple.emplace_back(field == "true" || field == "1"); break;
                     }
                 } catch (const std::invalid_argument& e) {
                     // Handle parsing errors gracefully
                     std::cerr << "Warning: Could not parse '" << field << "' for column " << colInfo.name << ". Skipping row." << std::endl;
                     return next(tuple); // Try to get the next valid row
                 }
            }
            return true; // Successfully produced a tuple
        }
        return false; // No more lines in the file
    }

    void close() override {
        if (fileStream_.is_open()) {
            fileStream_.close();
        }
    }

    const Schema& getSchema() const override { return qualifiedSchema_; }

private:
    std::string tablePath_;
    std::string alias_;
    const Catalog& catalog_;
    Schema qualifiedSchema_; // The output schema with aliased column names
    std::ifstream fileStream_;
};
// --- Select Operator ---
// Filters tuples based on a predicate expression.
class SelectOperator : public Operator {
public:
    SelectOperator(std::unique_ptr<Operator> input, std::unique_ptr<Expression> pred)
        : input_(std::move(input)), predicate_(std::move(pred)) {}

    // open() and close() simply pass the call down to the child.
    void open() override { input_->open(); }
    void close() override { input_->close(); }
    
    // The schema doesn't change through a select, so we just return our child's schema.
    const Schema& getSchema() const override { return input_->getSchema(); }

    bool next(Tuple& tuple) override {
        // Loop until we find a tuple that matches the predicate or the child runs out of data.
        while (input_->next(tuple)) {
            // Evaluate the predicate on the tuple we just got.
            Value result = predicate_->evaluate(tuple, getSchema());
            // If the expression evaluates to true, we've found our next tuple.
            if (std::get<bool>(result)) {
                return true; // Success! The tuple is passed to our caller.
            }
            // Otherwise, the loop continues to get the next tuple from the child.
        }
        return false; // Child has no more tuples.
    }

private:
    std::unique_ptr<Operator> input_;       // The operator that feeds us tuples.
    std::unique_ptr<Expression> predicate_; // The condition we check.
};


// --- Project Operator ---
// Transforms tuples by evaluating a list of expressions.
class ProjectOperator : public Operator {
public:
    // A helper struct to bundle an expression with its output alias.
    struct ProjExpr {
        std::string alias;
        std::unique_ptr<Expression> expr;
    };

    ProjectOperator(std::unique_ptr<Operator> input, std::vector<ProjExpr> exprs)
        : input_(std::move(input)), expressions_(std::move(exprs)) {
        
        // The Project operator defines a NEW schema based on its expressions.
        for (const auto& p_expr : expressions_) {
            // A real database would have a complex type inference system here.
            // For our project, we will simplify. Let's assume float for MUL
            // and infer from column refs, defaulting to string. This is a bit of a hack
            // but is sufficient for the specific queries you've provided.
            DataType type = DataType::STRING; 
            if (dynamic_cast<BinaryExpression*>(p_expr.expr.get())) {
                 type = DataType::FLOAT; // Assume MUL produces float
            } else if (auto* col_ref = dynamic_cast<ColumnRefExpression*>(p_expr.expr.get())) {
                 // We can't easily get the column type without an input tuple.
                 // This part of the system would need to be more sophisticated in a real DB.
                 // For now, let's keep it simple and fix if needed.
                 // The provided query schemas are simple enough this won't be an issue yet.
            }
            outputSchema_.addColumn(p_expr.alias, type); 
        }
    }

    void open() override { input_->open(); }
    void close() override { input_->close(); }
    const Schema& getSchema() const override { return outputSchema_; }

    bool next(Tuple& tuple) override {
        Tuple inputTuple;
        // First, get a tuple from our child.
        if (input_->next(inputTuple)) {
            tuple.clear(); // Clear the output tuple to build our new one.
            // Now, evaluate each of our expressions to build the new tuple.
            for (const auto& p_expr : expressions_) {
                tuple.push_back(p_expr.expr->evaluate(inputTuple, input_->getSchema()));
            }
            return true; // Successfully created a projected tuple.
        }
        return false; // Child has no more tuples.
    }

private:
    std::unique_ptr<Operator> input_;
    std::vector<ProjExpr> expressions_;
    Schema outputSchema_; // The new schema we produce.
};

// --- Limit Operator ---
// Stops producing tuples after a specified limit has been reached.
class LimitOperator : public Operator {
public:
    LimitOperator(std::unique_ptr<Operator> input, int limit)
        : input_(std::move(input)), limit_(limit), count_(0) {}
    
    void open() override { input_->open(); count_ = 0; }
    void close() override { input_->close(); }
    const Schema& getSchema() const override { return input_->getSchema(); }

    bool next(Tuple& tuple) override {
        // If we've already reached our limit, stop.
        if (count_ >= limit_) {
            return false;
        }
        // Try to get a tuple from our child.
        if (input_->next(tuple)) {
            count_++; // Increment our counter.
            return true; // And pass the tuple up.
        }
        return false; // Child is out of tuples.
    }

private:
    std::unique_ptr<Operator> input_;
    int limit_;
    int count_;
};


// --- Nested-Loop Join Operator ---
// Joins tuples from two inputs using a nested loop algorithm.
class NestedLoopJoinOperator : public Operator {
public:
    NestedLoopJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right, std::unique_ptr<Expression> cond)
        : left_(std::move(left)), right_(std::move(right)), condition_(std::move(cond)) {
        // The output schema is simply the left and right schemas merged.
        outputSchema_ = Schema::merge(left_->getSchema(), right_->getSchema());
    }

    void open() override {
        left_->open();
        right_->open();
        // "Prime the pump": get the first tuple from the left side to start the outer loop.
        hasLeftTuple_ = left_->next(leftTuple_);
    }

    bool next(Tuple& tuple) override {
        // The outer loop continues as long as we have a valid tuple from the left side.
        while (hasLeftTuple_) {
            Tuple rightTuple;
            // The inner loop: get the next tuple from the right side.
            if (right_->next(rightTuple)) {
                // Combine the current left and right tuples.
                Tuple combined = leftTuple_;
                combined.insert(combined.end(), rightTuple.begin(), rightTuple.end());
                
                // Check if the combined tuple satisfies the join condition.
                if (std::get<bool>(condition_->evaluate(combined, outputSchema_))) {
                    tuple = combined;
                    return true; // Found a match!
                }
            } else {
                // The right side is exhausted for the current left tuple.
                // Get the NEXT tuple from the left side.
                hasLeftTuple_ = left_->next(leftTuple_);
                // And RESET the right side to start its scan from the beginning.
                right_->close();
                right_->open();
            }
        }
        return false; // The left side is exhausted. The join is complete.
    }

    void close() override {
        left_->close();
        right_->close();
    }
    
    const Schema& getSchema() const override { return outputSchema_; }

private:
    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::unique_ptr<Expression> condition_;
    Schema outputSchema_;

    // State for the join algorithm
    Tuple leftTuple_;
    bool hasLeftTuple_ = false;
};

class BlockNestedLoopJoinOperator : public Operator {
public:
    BlockNestedLoopJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right, std::unique_ptr<Expression> cond, size_t blockSize = 100)
        : left_(std::move(left)), right_(std::move(right)), condition_(std::move(cond)), blockSize_(blockSize) {
        outputSchema_ = Schema::merge(left_->getSchema(), right_->getSchema());
    }

    void open() override {
        left_->open();
        right_->open();
        loadNextLeftBlock(); // Prime the pump with the first block
    }

    bool next(Tuple& tuple) override {
        while (!leftBlock_.empty()) {
            Tuple rightTuple;
            // Scan the right side for the current left tuple
            if (right_->next(rightTuple)) {
                const auto& leftTuple = leftBlock_[blockIndex_];
                Tuple combined = leftTuple;
                combined.insert(combined.end(), rightTuple.begin(), rightTuple.end());
                
                if (std::get<bool>(condition_->evaluate(combined, outputSchema_))) {
                    tuple = combined;
                    return true; // Found a match
                }
            } else { // Right side is exhausted for the current left tuple
                blockIndex_++; // Move to the next tuple in our block
                if (blockIndex_ >= leftBlock_.size()) {
                    // We've exhausted the current block, try to load a new one
                    if (!loadNextLeftBlock()) {
                        return false; // No more blocks to load from the left
                    }
                } else {
                    // Reset right side for the next tuple in our block
                    right_->close();
                    right_->open();
                }
            }
        }
        return false; // Left side is exhausted
    }

    void close() override {
        left_->close();
        right_->close();
    }
    
    const Schema& getSchema() const override { return outputSchema_; }

private:
    bool loadNextLeftBlock() {
        leftBlock_.clear();
        blockIndex_ = 0;
        Tuple temp;
        while (leftBlock_.size() < blockSize_ && left_->next(temp)) {
            leftBlock_.push_back(temp);
        }
        // Reset the inner loop (right side) for the new block
        right_->close();
        right_->open();
        return !leftBlock_.empty();
    }

    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::unique_ptr<Expression> condition_;
    Schema outputSchema_;

    // State for the BNLJ algorithm
    size_t blockSize_;
    std::vector<Tuple> leftBlock_;
    size_t blockIndex_ = 0;
};
// --- Hash Join Operator ---
// Performs an efficient equijoin by hashing one table and probing with the other.
class HashJoinOperator : public Operator {
public:
    HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right, 
                     std::unique_ptr<Expression> probeKey, std::unique_ptr<Expression> buildKey)
        : probe_(std::move(left)), build_(std::move(right)), 
          probeKeyExpr_(std::move(probeKey)), buildKeyExpr_(std::move(buildKey)) {
        outputSchema_ = Schema::merge(probe_->getSchema(), build_->getSchema());
    }

    void open() override {
        // 1. Build Phase: Read all tuples from the right input and build the hash table.
        hashTable_.clear();
        build_->open();
        Tuple buildTuple;
        while (build_->next(buildTuple)) {
            Value key = buildKeyExpr_->evaluate(buildTuple, build_->getSchema());
            hashTable_[key].push_back(buildTuple);
        }
        build_->close();

        // 2. Probe Phase Setup: Open the left input to prepare for probing.
        probe_->open();
        hasProbeTuple_ = false; // Ensure we fetch a new probe tuple first
    }

    bool next(Tuple& tuple) override {
        while (true) {
            // If we have a valid iterator, we have more matches for the current probe tuple.
            if (hasProbeTuple_ && matchIterator_ != matchEndIterator_) {
                const Tuple& buildTuple = *matchIterator_;
                tuple = probeTuple_;
                tuple.insert(tuple.end(), buildTuple.begin(), buildTuple.end());
                matchIterator_++; // Advance to the next match for this key
                return true;
            }

            // Otherwise, get the next tuple from the probe (left) side.
            hasProbeTuple_ = probe_->next(probeTuple_);
            if (!hasProbeTuple_) {
                return false; // Probe side is exhausted, join is complete.
            }

            // We have a new probe tuple; find its matches in the hash table.
            Value key = probeKeyExpr_->evaluate(probeTuple_, probe_->getSchema());
            auto range = hashTable_.find(key);
            if (range != hashTable_.end()) {
                // Found matches, set up iterators
                matchIterator_ = range->second.begin();
                matchEndIterator_ = range->second.end();
            } else {
                // No matches, create empty iterators
                matchIterator_ = end(emptyVec_);
                matchEndIterator_ = end(emptyVec_);
            }
        }
    }

    void close() override {
        probe_->close();
        // build_ is already closed after the build phase
    }

    const Schema& getSchema() const override { return outputSchema_; }

private:
    std::unique_ptr<Operator> probe_; // Left input
    std::unique_ptr<Operator> build_; // Right input
    std::unique_ptr<Expression> probeKeyExpr_;
    std::unique_ptr<Expression> buildKeyExpr_;
    Schema outputSchema_;

    // State for the hash join algorithm
    std::unordered_map<Value, std::vector<Tuple>> hashTable_;
    Tuple probeTuple_;
    bool hasProbeTuple_ = false;
    std::vector<Tuple> emptyVec_; // For failed lookups

    // Iterators to navigate matches for the current probe tuple
    std::vector<Tuple>::const_iterator matchIterator_;
    std::vector<Tuple>::const_iterator matchEndIterator_;
};
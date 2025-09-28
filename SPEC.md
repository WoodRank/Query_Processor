# Query Processor Project

## Overview
You will build a working relational query processor that evaluates JSON-serialized relational algebra expressions over CSV (comma-separated values) files using a Volcano-style iterator model. The project is divided into two parts so that you can focus first on the core select–project–join (SPJ) engine and then on extending your system with additional capabilities and an accompanying report.

## Deliverables at a Glance
- **Part 1 – Core Engine (Select–Project–Join basics)**: submit runnable code implementing the foundational operators (scan, select, project, join, limit) with expression support, along with a brief readme describing how to execute sample queries and evidence that the operators work end-to-end.
- **Part 2 – Enhancements & Report**: submit the full codebase including at least three additional features, benchmark scripts/results, and a 3–4 page report documenting design choices, performance findings, and your use of Generative AI.

## Part 1 – Core Engine (Select–Project–Join)

**Due Thursday, Sep 25, 11:59pm**

### Objectives
Deliver a minimal SPJ ("Select-Project-Join") query pipeline that can execute end-to-end workloads using the provided JSON algebra format.

### Required Functionality
Implement and demonstrate the following features:
- **Scan** operator that reads CSV files.
- **Select** operator that filters rows using predicates over expressions.
- **Project** operator that selects and/or renames columns.
- **Nested-loop equi-join** operator that combines two inputs on equality predicates.
- **Limit** operator that restricts the number of rows produced.
- **Expression evaluation** within predicates and projections, supporting:
  - column references and aliases
  - constants (strings and numbers)
  - arithmetic (`+, -, *, /`)
  - comparisons (`=, !=, <, <=, >, >=`)
  - logical operations (`AND, OR, NOT`)

### Suggested Validation
Run at least a handful of JSON queries that each exercise multiple operators (e.g., scan → select → join → project) and verify the outputs, optionally by comparing with a reference engine such as SQLite or DuckDB.

### Submission Package
Provide a zip file or repository link containing:
- Source code for the core operators.
- Sample CSV + JSON query pairs you used for testing.
- Their companion schema files (e.g., `customers.schema.json`) so I can load the same catalog definitions without inferring types from CSV.
- A short readme (1–2 paragraphs) describing how to run the system and summarizing your validation approach.

## Part 2 – Enhancements & Report

**Due Thursday, Oct 2, 11:59pm**

### Objectives
Extend your engine beyond the basic SPJ pipeline, quantify its performance, and reflect on your implementation process.

### Additional Functionality
Choose **three** features from the list below (each counts as one feature unless otherwise noted). Feel free to implement more if time permits.
- Vectorized operators (operate on columnar/batch vectors instead of single rows).
- Additional join algorithms (each implemented variant counts as one feature):
  - Block nested-loop join
  - Sort-merge join
  - Hash join
  - Index nested-loop join
- Query optimizations:
  - Predicate pushdown
  - Projection pushdown
  - Join reordering using heuristics or a simple cost model
- Index scan operator (e.g., B+ tree or hash index)
- Develop a cost model to estimate query costs; evaluate the correlation between estimated cost and query execution time.

### Report Requirements (3–4 pages, PDF)
Include the following sections:
- Architecture overview with a diagram illustrating key components and their interactions.
- Discussion of at least three significant design or implementation decisions and their trade-offs.
- Description of the three additional features you implemented and how they integrate with the core engine.
- Benchmark methodology: at least three JSON queries, datasets used, measurement setup, and results. Present results via tables or plots and interpret what they show about your system.
- Reflection on the use of Generative AI tools:
  - Tools you used and what you used them for.
  - At least one representative prompt/response pair.
  - A paragraph discussing where GenAI helped, where it fell short, and how you would adjust your usage in future work.

### Submission Package
Provide:
- Updated source code including the additional features and any scripts/notebooks used for benchmarking.
- CSV datasets, their schema JSON companions, and the JSON queries used for evaluation (or instructions for obtaining them).
- Benchmark outputs (tables/plots) referenced in the report (included in the PDF -- do not submit separately).
- The final 3–4 page PDF report.

## Architecture and Interface Guidelines
You may design any interface you prefer, such as a command-line tool that reads JSON and CSV files, a REPL, or a batch benchmarking script. There are no restrictions on programming language or libraries. You may rely on libraries for tasks such as JSON/CSV parsing or data structures, but implement the core relational operators and control flow yourself (or with the help of Generative AI, which must be documented in the report).

## JSON-Encoded Relational Algebra
A JSON encoding of relational algebra expressions is provided in `JSON.md`. This format simplifies parsing by eliminating the need for a SQL parser. Inputs can be assumed to be type- and schema-correct, though you will need a lightweight catalog to handle table/column aliases. Sample datasets plus ready-to-run plans and schema descriptors are available in `example1/` (single-table SP) and `example2/` (multi-table SPJ) to jump-start your testing.

## Ensuring Correctness
Throughout both parts, verify that your system produces correct results across a range of test queries. Comparing outputs against a reference engine (e.g., SQLite or DuckDB) is a practical way to build confidence. Consider building automated tests so regressions are caught when you add features in Part 2.

## Hints

Feeling stuck? Or don't know where to start? Here are some hints to help you along the way.

**What should I do first?**
I recommend starting with the simplest operator: Scan. Implement it, test it, and make sure it works correctly. Then move on to Select, Project, Join, and finally Limit. Implement each operator one at a time, testing as you go.

**How should I structure my code?**
Look at the slides where we talked about the Volcano iterator model. You can create a base `Operator` class with methods like `open()`, `next()`, and `close()`. Each operator (Scan, Select, Project, Join, Limit) can inherit from this base class and implement these methods.

**How do I handle expressions?**
You can create a separate class or set of functions to evaluate expressions. This class can take an expression object (like the ones in the JSON plans) and evaluate it against a given row of data. Make sure to handle different types of expressions (column references, constants, arithmetic, comparisons, logical operations) as specified in the project requirements.

**How do I parse the JSON plans?**
You can use a JSON parsing library in your programming language of choice (like `json` in Python or `json` module in Node.js). Write a function that takes a JSON plan and recursively constructs the operator tree. Each operator in the JSON can be mapped to a corresponding class in your code.

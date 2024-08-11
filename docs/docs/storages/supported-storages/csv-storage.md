# CSV Storage

Introducing `CSVStorage`: a utility to process *.csv files, enabling SQL-like query operations such as SELECT, INSERT, and UPDATE.

## Key Features:

1. **SQL Queries on CSV**: Directly parse and operate on *.csv files using familiar SQL query operations.

2. **Optional Schema Support**: An associated schema can be provided for each CSV file. For instance, for a data file named `Book.csv`, its corresponding schema file should be named `Book.sql`.
   - If an associated schema file is found, it will be read and applied.
   - In the absence of a schema file, the first row of the data file will be treated as column headers and all column types will default to TEXT.

3. **Type Info File for Schemaless Data**: An auxiliary types file (`*.types.csv`) can be used to support data type recognition for schemaless data.
   - For a CSV data file named `Book.csv`, its corresponding types file will be `Book.types.csv`.
   - The types file will have a 1:1 mapping with the CSV data file entries, specifying the data type for each entry in alignment with the GlueSQL conventions.

I'd like to add a "vector" feature to the project. 

This feature will allow users to store and manipulate arrays of floating-point numbers within the database. The float-number vector will be represented as a custom data type, enabling efficient storage and retrieval of numerical data for applications such as machine learning, scientific computing, and data analysis.

I want you to create a plan for implementing this feature, including the following aspects:
1. **Data Model**: Define how the float-number vector will be represented in the database schema. Considerations include:
   - Data type definition (e.g., `VECTOR`)
   - Storage format (e.g., fixed-size array, variable-length array)
   - Constraints (e.g., maximum length, element type)

2. **Query Language Support**: Outline how users will interact with the float-number vector through SQL queries. This includes:
   - Syntax for creating tables with float-number vector columns
   - Insertion and retrieval of float-number vector data
   - Supported operations (e.g., vector addition, scalar multiplication, dot product)
   - Example queries demonstrating these operations

3. **Storage Implementation**: Describe how the float-number vector will be stored in the underlying storage system. Considerations include:
   - Efficient serialization and deserialization methods
   - Indexing strategies for fast retrieval
   - Handling of null or missing values

4. **Performance Considerations**: Identify potential performance bottlenecks and strategies to mitigate them. This includes:
   - Memory usage optimization
   - Query execution speed
   - Scalability for large datasets
   - Benchmarking plans to measure performance impact

Ultra-think step-by-step and provide a comprehensive implementation plan for the float-number vector feature in the database system. 
Write your plan in markdown format so that you can resume session by session.

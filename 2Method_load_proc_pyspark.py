###
# Considering the below code is for the procedure

# CREATE PROCEDURE UpsertEmployeeDetails (
#     IN emp_id INT,
#     IN emp_name VARCHAR(255),
#     IN salary DECIMAL(10, 2),
#     IN department_id INT
# )
# BEGIN
#     -- Check if the employee already exists
#     IF EXISTS (SELECT 1 FROM employees WHERE emp_id = emp_id) THEN
#         -- Update the existing record
#         UPDATE employees
#         SET emp_name = emp_name, salary = salary, department_id = department_id
#         WHERE emp_id = emp_id;
#     ELSE
#         -- Insert a new record if it doesn't exist
#         INSERT INTO employees (emp_id, emp_name, salary, department_id)
#         VALUES (emp_id, emp_name, salary, department_id);
#     END IF;
# END;

###

### Method 1: (This method uses the spark sql)

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder.appName("UpsertExampleSQL").getOrCreate()

# Load the employees data (assuming a JDBC source or preloaded DataFrame)
employees = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost/company_db") \
    .option("dbtable", "employees") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

# Register employees DataFrame as a temporary view
employees.createOrReplaceTempView("employees_temp")

# Function to perform upsert via SQL
def upsert_employee(emp_id, emp_name, salary, department_id):
    # Define the SQL query for the upsert
    upsert_sql = f"""
    MERGE INTO employees_temp AS target
    USING (SELECT {emp_id} AS emp_id, '{emp_name}' AS emp_name, {salary} AS salary, {department_id} AS department_id) AS source
    ON target.emp_id = source.emp_id
    WHEN MATCHED THEN
        UPDATE SET target.emp_name = source.emp_name, target.salary = source.salary, target.department_id = source.department_id
    WHEN NOT MATCHED THEN
        INSERT (emp_id, emp_name, salary, department_id)
        VALUES (source.emp_id, source.emp_name, source.salary, source.department_id)
    """
    
    # Execute the SQL query for the upsert
    spark.sql(upsert_sql)

# Example of calling the upsert function
upsert_employee(101, "John Doe", 55000.00, 2)


#### Method 2:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("UpsertExample").getOrCreate()

# Assuming we already have data loaded in the DataFrame
employees = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost/company_db") \
    .option("dbtable", "employees") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

# Function to perform the upsert
def upsert_employee(emp_id, emp_name, salary, department_id):
    # Check if the employee already exists
    existing_employee = employees.filter(col("emp_id") == emp_id).count()

    if existing_employee > 0:
        # Update the existing record
        employees.filter(col("emp_id") == emp_id) \
            .withColumn("emp_name", lit(emp_name)) \
            .withColumn("salary", lit(salary)) \
            .withColumn("department_id", lit(department_id)) \
            .write.format("jdbc").mode("overwrite").save()
    else:
        # Insert a new record
        new_employee = spark.createDataFrame([(emp_id, emp_name, salary, department_id)], ["emp_id", "emp_name", "salary", "department_id"])
        new_employee.write.format("jdbc").mode("append").save()

# Call the function with an example
upsert_employee(101, "John Doe", 55000.00, 2)

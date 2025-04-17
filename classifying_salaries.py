#You’re given a dataset containing employee names, departments, and their salaries.
 #Your task is to rank the employees by salary within each department and classify them as:
#"High Growth" if the salary is higher than the previous person in that department
#"Low Growth" if the salary is lower
#"No Growth" if the salary is the same or it's the first employee in the department

# 𝐃𝐚𝐭𝐚 (Input Schema)
#data = [
 #("John", "HR", 60000),
# ("Jane", "HR", 65000),
 #("Jake", "HR", 60000),
# ("Alice", "IT", 80000),
 #("Bob", "IT", 90000),
 #("Charlie", "IT", 85000),
#]
#columns = ["name", "department", "salary"]

from pyspark.sql import SparkSession from pyspark.sql.window import Window
from pyspark. sql. functions import lag, when, col
# Step 1: Start Spark session
spark = SparkSession. builder.appName"SalaryGrowth"). getOrCreate()
# Step 2: Create DataFrame
data = [
("John", "HR", 60000),
("Jane","HR", 65000),
("Jake","HR"', 60000),
("Alice", "IT"', 80000),
("Bob", "IT", 90000),
("Charlie", "IT", 85000),
]
columns = ["name", "department", "salary"]
df = spark. createDataFrame(data, columns)
# Step 3: Define window and use LAG to compare salary
windowSpec = Window. partitionBy"department"). orderBy ("salary")
df_with_lag = df withColumn("prev_salary", lag ("salary"). over(windowSpec))
# Step 4: Apply classification logic
final_df = df_with_lag.withColumn(
"growth_status",
when (col ("prev_salary"). isNull(), "No Growth")
•when (col ("salary") > col("prev_salary"), "High Growth")
•when (col("salary") < col("prev_salary"), "Low Growth" )
•otherwise ("No Growth")
)
# Display final result
final_df. show()

https://media.licdn.com/dms/image/v2/D5622AQHGaFkUgSu_GQ/feedshare-shrink_1280/B56ZY1bSbBGQAo-/0/1744653077204?e=1747872000&v=beta&t=BlZg0UsXX5SzABJEJGE7rAmw5XRRMzpD1qdREMcwd0o

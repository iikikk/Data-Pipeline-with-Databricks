# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Reading the CSV Data

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .option("inferSchema", "true") \
    .load("/Volumes/ids706_data_engineering/default/qc57_wine/winequality-red.csv")

# Display the DataFrame
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing the Data
# MAGIC Data Cleaning
# MAGIC - Check for Missing Values
# MAGIC - Handle Missing Values (if any)

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Sum null values in each column
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()
df_clean = df.dropna()

# COMMAND ----------

# MAGIC %md
# MAGIC Feature Engineering
# MAGIC
# MAGIC Create a New Feature:
# MAGIC
# MAGIC create an acidity_level feature based on the pH value.

# COMMAND ----------


from pyspark.sql.functions import when

df_with_features = df_clean.withColumn(
    "acidity_level",
    when(col("pH") < 3.0, "High")
    .when((col("pH") >= 3.0) & (col("pH") <= 3.5), "Medium")
    .otherwise("Low")
)
df_with_features.describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC Writing Data to a Sink

# COMMAND ----------

# Define the output path
output_path = "/Volumes/ids706_data_engineering/default/qc57_wine/processed_winequality_data.parquet"

# Write the DataFrame in Parquet format
df_with_features.write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the Output

# COMMAND ----------

# Read the processed data
processed_df = spark.read.parquet(output_path)
display(processed_df)

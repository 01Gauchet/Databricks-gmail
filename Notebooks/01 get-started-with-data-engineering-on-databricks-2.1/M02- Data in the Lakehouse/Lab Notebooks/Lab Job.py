# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Lab Job
# MAGIC This notebook is to be run as part of the Comprehensive Lab at the end of the course. You do not need to run this notebook outside of the lab.
# MAGIC

# COMMAND ----------

# MAGIC %run ../../Includes/_common

# COMMAND ----------

lesson_name = "lab_job"
DA = DBAcademyHelper(course_config=course_config,
                     lesson_config=lesson_config)

# COMMAND ----------

query = f"UPDATE {DA.catalog_name}.job_lab_schema.promotion_data SET promotion_type = '4' WHERE promotion_type = '2'"

# Execute the query and store the result
query_result = spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Get Started with Databricks for Data Engineering
# MAGIC
# MAGIC In this course, you will learn basic skills that will allow you to use the Databricks Data Intelligence Platform to perform a simple data engineering workflow. You will be given a tour of the workspace, and you will be shown how to work with notebooks. You will create a basic data engineering workflow while you perform tasks like creating and using compute resources, working with repositories, and creating and managing basic workflow jobs. The course will also introduce you to Databricks SQL. Finally, you will see how data is stored, managed, governed, and secured within the lakehouse.
# MAGIC
# MAGIC
# MAGIC ## Course agenda
# MAGIC
# MAGIC | Time | Module &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Lessons &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; |
# MAGIC |:----:|-------|-------------|
# MAGIC | 80m    | **[Introduction to Data Engineering on Databricks]($./M01- Introduction to Data Engineering on Databricks)**    | Databricks Fundamentals<br>[Demo: The Databricks Platform]($./M01- Introduction to Data Engineering on Databricks/1.1 - The Databricks Platform) </br> Introduction to Repos on Databricks </br> [Demo: Working with Repos on Databricks]($./M01- Introduction to Data Engineering on Databricks/1.2 - Working with Repos on Databricks) </br> Introduction to Compute Resources </br> [Lab: Working with Compute Resources]($./M01- Introduction to Data Engineering on Databricks/1.3L - Working with Compute Resources)<br> Databricks Notebooks </br> [Demo: Working with Notebooks]($./M01- Introduction to Data Engineering on Databricks/1.4 - Working with Notebooks) </br> [Lab: Comprehensive Lab]($./M01- Introduction to Data Engineering on Databricks/1.5L - Comprehensive Lab)|
# MAGIC | 90m    | **[Data in the Lakehouse]($./M02- Data in the Lakehouse)** | Data Storage and Delta Lake </br> Unity Catalog </br> [Demo: Data Management]($./M02- Data in the Lakehouse/2.1 - Data Management) </br> [Demo: Data Governance and Security]($./M02- Data in the Lakehouse/2.2 - Data Governance and Security) </br> Introduction to Workflows </br> [Demo: Using Workflows]($./M02- Data in the Lakehouse/2.3 - Using Workflows) </br> Databricks SQL for Data Engineers </br> [Demo: Using Databricks SQL]($./M02- Data in the Lakehouse/2.4 - Using Databricks SQL) </br> [Lab: Capstone Lab]($./M02- Data in the Lakehouse/2.5L - Capstone Lab) |

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use one of the following Databricks runtime(s): **14.3.x-scala2.12, 14.3.x-photon-scala2.12, 14.3.x-cpu-ml-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

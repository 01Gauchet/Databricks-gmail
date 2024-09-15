# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Capstone Lab
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC * Demonstrate how to create a complete data engineering workflow in the Databricks Data Intelligence Platform
# MAGIC
# MAGIC **Project Overview**: Telcom Inc., an internet service provider, plans on running a series of promotions aimed at increasing their customer base. As part of these promotions, they will collect data that will be used to determine whether the promotion was successfull.
# MAGIC
# MAGIC **Platform**: Telcom Inc. has chosen Databricks Data Intelligence platform. Data is collected as comma-separated, plain text files. As a data engineer, your mission is to setup a table used to hold this data, grant permissions on that table to other account users, ingest the data into Databricks, and schedule a job that runs a notebook used for future tasks.
# MAGIC
# MAGIC ### Dataset
# MAGIC The dataset provided comprises simple promotion details that will be expanded as time goes on. Below, you'll find a setup script that downloads the files in CSV format and provides the file paths.
# MAGIC
# MAGIC ### Project Tasks
# MAGIC Below, are the tasks you will need to complete in order to finish this project. If you get stuck, please look back at course materials.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the Setup Script
# MAGIC The first step is to run the classroom setup script to import the data files into your workspace and to setup temporary paths. This is a setup that we need to complete as part of the lab. This script will establish necessary configuration variables tailored to each user. It is not a step you would normally have to run in the real world.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11

# COMMAND ----------

# MAGIC %md
# MAGIC ### Other Conventions:
# MAGIC Throughout this lab, we'll make use of the object `DA`, which provides critical variables. Execute the code block below to see various variables that will be used in this notebook.

# COMMAND ----------

print(f"Username:             {DA.username}")
print(f"Catalog Name:         {DA.catalog_name}")
print(f"Schema Name:          {DA.schema_name}")
print(f"Working Directory:    {DA.paths.working_dir}")
print(f"Dataset Location:     {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1: Create a table
# MAGIC We need to create a table we will use to ingest data from our `.csv` files. We will share this table with other account users and run a job on this table in future tasks. Write SQL code in the cell below that creates a Delta table named **`promotion_data`** that we will use in this lab. Run the following cell to verify you completed this task successfully.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE 
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the cell below to verify you did this correctly

# COMMAND ----------

expected_table = lambda: spark.table("promotion_data")
suite = DA.tests.new("Table Creation Verification")
suite.test_not_none(lambda: expected_table(), "Table \"promotion_data\" is created")

suite.display_results()
assert suite

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: Grant select permission
# MAGIC We have been asked to share the table we created above with all members of our account. Grant the ability to run **SELECT** on the table to all account users. Then, run the following cell to check your work.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Follow the instructions in the notebook, "2.2 - Data Governance and Security," if you need to review how this is done.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the following cell to check your work.

# COMMAND ----------

# Check if the grant was successful
result = spark.sql("SHOW GRANT ON TABLE promotion_data")
assert  not result.isEmpty(), "GRANT was not performed correctly. Try again."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: Ingest Data into the Table
# MAGIC Update everywhere you see "FILL_IN" in the code below so that the cell copies data from the file, `/databricks-datasets/retail-org/promotions/promotions.csv`, into the table, `promotion_data`.  
# MAGIC   
# MAGIC Ensure `FORMAT_OPTIONS` and `COPY_OPTIONS` have the proper key/value pairs. You can then run the following cell to check your work.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COPY INTO FILL_IN
# MAGIC   FROM 'FILL_IN'
# MAGIC   FILEFORMAT = FILL_IN
# MAGIC   FORMAT_OPTIONS ('FILL_IN')
# MAGIC   COPY_OPTIONS ('mergeSchema' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Double-check your work by running the following cell

# COMMAND ----------

suite = DA.tests.new("Data Ingestion")

# Verify data is loaded successfully
expected_table = lambda: spark.table("promotion_data")
suite.test_true(lambda: expected_table().count() > 0, "Data loaded into the table")

suite.display_results()
assert suite.passed

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: Create a Scheduled Job
# MAGIC We plan on regularly running a job on this table. Although we have not written the specific tasks that the job will complete, we can setup the job to run a notebook that we can update with the specific requirements in the future. So for now, we are just going to configure the job to run a specific notebook.
# MAGIC
# MAGIC Configuring this job will require parameters unique to a given user.
# MAGIC
# MAGIC Run the cell below to print out values you'll use to configure your job. Then, using the values printed below, configure a job with that notebook task. Use the "Notebook Path" as the notebook task.
# MAGIC

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

Using the parameters above, configure a job with the notebook task. The path above is the notebook you should use to configure the notebook task.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use the cell below to verify that you configured the job correctly

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: Run the Job
# MAGIC Now, run the job that you configured in the previous step. This job will take a short time to run after the job provisions compute. You can check your work by running the following cell.

# COMMAND ----------

Run the job you configured above

# COMMAND ----------

# MAGIC %md
# MAGIC ### Double Check Your Work
# MAGIC The cell below will verify that you were able to successfully run the job you created.

# COMMAND ----------

# Check that the query result is less than 4 rows
query_result = spark.sql("SELECT * FROM promotion_data WHERE promotion_type = '4'")
assert query_result.count() > 0, "There must have been a problem with the job run"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: Create Visualization:
# MAGIC 1. Run the SQL query to get the necessary data for the bar chart.
# MAGIC 2. Click the (+) icon at the top of your table in the cell output.
# MAGIC 3. Choose visualization from the dropdown.
# MAGIC 4. Select Bar as your visualization type
# MAGIC 5. Pick the bar chart column types and save your visualization.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  promotion_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!!
# MAGIC You have successfully completed the capstone lab! Now, Telcom, Inc. has the ability to ingest their promotion data, run a job on that data and create a visualization from that data. Additionally, everyone in the workspace has access to this data. Way to go!!

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Clean up Classroom
# MAGIC
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

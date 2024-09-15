# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Lab: Compute Resources
# MAGIC In order to work with data in Databricks, we need to use compute resources (i.e. clusters). In this lab, we will launch a new cluster.
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC * Create and Launch a new cluster
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Cluster
# MAGIC
# MAGIC **Note**: Select 14.3 LTS Databricks Runtime version to create the cluster.
# MAGIC
# MAGIC Before working with notebooks, let's ensure we have a suitable compute environment. We will create a cluster with the necessary runtime configuration. Follow these steps:
# MAGIC
# MAGIC 1. Click on **Compute** in the left navigation bar.
# MAGIC
# MAGIC 2. Click the blue **Create with DBAcademy** button.
# MAGIC
# MAGIC 3. Please provide a suitable name for the cluster, such as "username's DBAcademy Cluster" (The name of the cluster is predefined, but you can change it if you prefer).
# MAGIC
# MAGIC 4. Choose the **Standard Cluster** on the Databricks runtime version dropdown.
# MAGIC
# MAGIC 5. Click **Create compute** to create the cluster.
# MAGIC
# MAGIC **NOTE:** Clusters can take several minutes to deploy and startup. Once you have finished deploying a cluster, feel free to continue to explore the compute creation UI.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### <img src="https://files.training.databricks.com/images/icon_warn_24.png"> Single User Cluster Required for this Course
# MAGIC **IMPORTANT:** This course requires you to run notebooks on a single user cluster. 
# MAGIC
# MAGIC Follow the instructions above to create a cluster that has **Access mode** set to **`Single User`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC Now that we have created our compute resources, we can begin running notebooks and queries.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>

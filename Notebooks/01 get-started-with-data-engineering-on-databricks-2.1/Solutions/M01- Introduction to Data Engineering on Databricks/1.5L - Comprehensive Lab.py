# Databricks notebook source
# MAGIC %md
# MAGIC #Comprehensive Lab
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lab you:<br>
# MAGIC
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC 1. Navigate the Databricks workspace and create assets effectively.
# MAGIC 1. Integrate GitHub within the Databricks workspace using Personal Access Tokens (PAT).
# MAGIC 1. Perform Git operations such as fork, clone, push, pull, and commit changes with the cloned repository within the Databricks workspace.
# MAGIC 1. Work with Databricks notebooks, including attaching the notebook to the latest 14.3 LTS Databricks Runtime (DBR).
# MAGIC 1. Write code in multiple languages including SQL, Python, and Markdown notes within Databricks notebooks.
# MAGIC
# MAGIC #### These objectives aim to provide you with a comprehensive understanding and hands-on experience of using the Databricks workspace, GitHub integration, and multi-language coding within Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Navigating Workspace and creating assets
# MAGIC ### Step 1: Exploring the Workspace Page
# MAGIC - Click on workspace in the sidebar menu. Explore different sections in the workspace page - Home, workspace, Repos, Trash and favourites.
# MAGIC
# MAGIC ###Step 2: Create a Folder 
# MAGIC - Select the folder under home or workspace section where you want to create a new folder. 
# MAGIC - Click the Kebab menu **( ⋮ )** at the right-corner, choose **Create > Folder**
# MAGIC - Provide the name **"Get Started"** for the folder and click on create 
# MAGIC - Alternatively, click on **"Create button"** at the top-right corner of the workspace page. 
# MAGIC - Select Folder option, Name it and click on create. 
# MAGIC
# MAGIC ###Step 3: Create a Notebook 
# MAGIC - Navigate to the desired folder
# MAGIC - Click the Kebab menu **( ⋮ )** at the right-corner, choose **Create > Notebook**
# MAGIC - Alternatively, click on the **Create** button inside the desired folder
# MAGIC - Select **notebook** option to create new notebook 
# MAGIC - Name the notebook **"Get Started with Databricks"** by clicking on the Untitled name.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Repos and Git Integration
# MAGIC ### Step 1: Create a PAT in Github and attach it to Databricks workspace.
# MAGIC - Click [here](https://github.com/) to navigate to Github
# MAGIC ### Step 2: Fork and Clone the Public Repo into your workspace Repo folder 
# MAGIC - Click [here](https://github.com/databricks-academy/get-started-with-data-engineering-on-databricks-repo-example) to access the public repo for this lab.
# MAGIC ### Step 3: Describe Git Integration Processes
# MAGIC #### 1. Comparing Changes:
# MAGIC - Open a new notebook or file from the newly added Git repository.
# MAGIC - Make changes to the Notebook content.
# MAGIC - Describe the process of comparing changes between the notebook in Databricks and the corresponding Git repository.
# MAGIC      
# MAGIC #### 2. Pulling Changes:
# MAGIC - Inside the cloned repo folder, click the kebab-menu **( ⋮ )** or right-click on the folder name.
# MAGIC - Select `Git...` from the dropdown menu.
# MAGIC - Click on the `Pull` button at the top-right corner to update the repo with the latest changes.
# MAGIC
# MAGIC #### 4. Committing and Pushing the Changes:
# MAGIC - Inside the cloned repository folder, click the `Git...` button.
# MAGIC - Select the branch where you want to make your changes.
# MAGIC - Enter a commit message describing your changes.
# MAGIC - Choose Commit and Push button to save your changes along with the commit message.
# MAGIC
# MAGIC **Note**: If needed, refer back to the material **"Demo 1.4: Working with Repos on Databricks"**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Working with Notebooks 
# MAGIC Note: Before you work on task 3 make sure you attach the notebook to the cluster by clicking on the **connect button** 
# MAGIC - Use the latest **DBR 14.3 LTS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Write and execute code using multiple languages.
# MAGIC
# MAGIC #### a. Run Python code
# MAGIC 1. In the below cell, write python cmd to print "Welcome to Databricks for Data Engineering".
# MAGIC 2. Run the cell, to execute the output

# COMMAND ----------

# TODO 
# print "Welcome to Databricks for Data Engineering"
# <FILL_IN>

# COMMAND ----------

# print "Welcome to Databricks for Data Engineering"
print("Welcome to Databricks for Data Engineering")

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Run SQL code
# MAGIC 1. Add %sql to the first line of the cell to work with SQL in non-sql notebooks.
# MAGIC 2. Write sql query to print today's date in `'d MMM yyyy'` format.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO 
# MAGIC -- Print today's date in 'd MMM yyyy' format.
# MAGIC -- <FILL_IN>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Print today's date in 'd MMM yyyy' format.
# MAGIC SELECT date_format(getdate(), 'd MMM yyyy') AS Today;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write Markdown based notes 
# MAGIC 1. Use magic command **&percnt;md** in the first line of the cell to render the cell as Markdown.
# MAGIC 2. **Headings**: Create different levels of headings using hash symbols (#). For example, # Heading 1 for a level 1 heading, ## Heading 2 for a level 2 heading, and so on up to ###### Heading 6 for a level 6 heading.
# MAGIC 3. **Links**: Create links using the following syntax: - `[link text](URL)`. Use the given URL to create Databricks link (https://www.databricks.com)
# MAGIC 4. **Lists**: 
# MAGIC * Create unordered lists using asterisks, hyphens, or plus signs. For example, * Item 1 will create a bullet point. 
# MAGIC * Create ordered lists using numbers followed by periods. For example, 1. Item 1.
# MAGIC 5. **Bold and Italic Text**: 
# MAGIC * Create bold text using double asterisks or underscores. For example, `**bold text**` or `__bold text__`. 
# MAGIC * Create italic text using single asterisks or underscores. For example, `*italic text*` or `_italic text_`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC Congratulations! You have completed the basic tasks present in the first module of this course, which gives you hands-on experience using the Databricks workspace before we attempt to finish the simple Data Engineering workflow.

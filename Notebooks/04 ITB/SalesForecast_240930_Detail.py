# Databricks notebook source
# MAGIC %md
# MAGIC # KID Sales Forecasting project

# COMMAND ----------

import pandas as pd
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Quality**
# MAGIC
# MAGIC - Check for NULL values:

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC FROM main.itb_forecasting.forecast_main_detail

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.itb_forecasting.forecast_main_detail limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.itb_forecasting.forecast_main
# MAGIC WHERE TBL_FORECAST_START_DATE IS NULL 
# MAGIC    OR TBL_FORECAST_END_DATE IS NULL 
# MAGIC    OR TBL_FORECAST_BILLING IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC - Validate Forecast Dates: Ensure the forecast start date is earlier than the end date.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.itb_forecasting.forecast_main
# MAGIC WHERE TBL_FORECAST_START_DATE > TBL_FORECAST_END_DATE;

# COMMAND ----------

# MAGIC %md
# MAGIC - Check for Negative Values in Billing:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM main.itb_forecasting.forecast_main
# MAGIC WHERE TBL_FORECAST_BILLING < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Time-Based Features**:
# MAGIC
# MAGIC - **Forecast Duration**: The difference between **TBL_FORECAST_END_DATE** and **TBL_FORECAST_START_DATE** to measure how long the forecast lasts.
# MAGIC - **Time Remaining**: The difference between **TBL_FORECAST_END_DATE** and the current date to estimate how many days are left in the forecast period
# MAGIC
# MAGIC ### **Financial Features**:
# MAGIC - **Expected Revenue**: A derived feature using **TBL_FORECAST_FULL_VALUE** and **TBL_FORECAST_PROBABILITY** to compute the expected value of the contract.
# MAGIC - **Revenue per FTE**: Calculating how much revenue is generated per Full-Time Equivalent resource.
# MAGIC - **Rate Analysis**: Effective Rate per Hour: Using TBL_FORECAST_BILLING and TBL_FORECAST_HOURS to determine the effective rate being charged per hour

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC TBL_FORECAST_CREATED
# MAGIC ,DATEDIFF(day, TBL_FORECAST_DETAIL_START_DATE, TBL_FORECAST_DETAIL_END_DATE) AS Forecast_Duration
# MAGIC ,DATEDIFF(day, GETDATE(), TBL_FORECAST_DETAIL_END_DATE) AS Days_Remaining
# MAGIC ,(TBL_FORECAST_DETAIL_FULL_VALUE * TBL_FORECAST_DETAIL_PROBABILITY / 100) AS Expected_Revenue
# MAGIC ,TBL_FORECAST_DETAIL_BILLING / NULLIF(TBL_FORECAST_DETAIL_FTE, 0) AS Revenue_per_FTE
# MAGIC ,TBL_FORECAST_DETAIL_BILLING / NULLIF(TBL_FORECAST_DETAIL_HOURS, 0) AS Effective_Billing_Rate
# MAGIC ,TBL_FORECAST_DETAIL_BILLING / NULLIF(DATEDIFF(day, TBL_FORECAST_DETAIL_START_DATE, TBL_FORECAST_DETAIL_END_DATE), 0) AS Revenue_per_Day
# MAGIC ,COUNT(TBL_FORECAST_CONSULTANT) OVER (PARTITION BY TBL_FORECAST_CONSULTANT) AS Consultant_Project_Count
# MAGIC
# MAGIC from main.itb_forecasting.forecast_main_detail
# MAGIC where TBL_FORECAST_CREATED >= '2023-01-01'

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Client and Partner Insights**:
# MAGIC
# MAGIC - **Client Repeatability**: A feature counting how many times the same client has appeared in the forecasts. This could provide insights into recurring clients.
# MAGIC - **Forecast Success Indicator**: A binary feature that indicates whether a forecast was successfully realized (e.g., TBL_FORECAST_STAGE = 'Closed' or TBL_FORECAST_PROBABILITY >= 90).
# MAGIC - **Consultant Involvement**: Consultant Load: Calculating how many forecasts a consultant is involved in to measure workload distribution.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC COUNT(TBL_FORECAST_CLIENT) OVER (PARTITION BY TBL_FORECAST_CLIENT) AS Client_Repeatability
# MAGIC ,CASE 
# MAGIC    WHEN TBL_FORECAST_PROBABILITY >= 90 THEN 1 
# MAGIC    ELSE 0 
# MAGIC END AS Forecast_Success
# MAGIC ,COUNT(TBL_FORECAST_CONSULTANT) OVER (PARTITION BY TBL_FORECAST_CONSULTANT) AS Consultant_Load
# MAGIC from main.itb_forecasting.forecast_main

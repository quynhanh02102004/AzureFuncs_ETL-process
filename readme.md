# Cloud Computing Data Warehousing and Visualization for UK Traffic Accident Analysis: A Microsoft Azure Approach

This project outlines the development of an end-to-end, cloud-based Business Intelligence (BI) system using Microsoft Azure to analyze UK traffic accident data. The system focuses on data warehousing, automated ETL processes, and interactive data visualization.

## CHAPTER 1. INTRODUCTION

This project introduces an initiative to construct a comprehensive, cloud-based Business Intelligence system leveraging Microsoft Azure for the analysis of UK traffic accidents. The primary **objectives** are threefold: firstly, to **develop a robust cloud-based data warehouse** that centralizes accident data for structured analysis and implements an ETL (Extract, Transform, Load) solution on the Azure Functions platform, adhering to a Data Lakehouse architecture with distinct Bronze, Silver, and Gold layers. Secondly, the project aims to **implement event-driven data processing**, utilizing Azure Functions triggers to ensure real-time data ingestion, facilitate scheduled updates, and fully automate the ETL workflow across all data layers. Finally, a key goal is to **build interactive dashboards** using Power BI, designed to align with business needs and provide stakeholders with actionable insights into accident trends and high-risk locations, thereby supporting data-driven policy decisions. The **business context** for this system stems from the need to understand complex traffic accident patterns, identify the impact of various uncontrollable factors such as location and weather, and analyze controllable causes like light conditions and road types to ultimately enhance road safety. To effectively monitor progress and measure success, the project defines several crucial **Key Performance Indicators (KPIs)**, including Total Accidents to track accident frequency, Total Fatalities to monitor severity, the Serious Accident Rate (SAR) to assess the proportion of severe incidents, and the Average Speed Limit (AVGSpeed) to evaluate traffic control measures.

## CHAPTER 2. DATA PREPARATION AND DATA MODELING

### 2.1. Data source

*   **Data collection:** Utilizes a comprehensive dataset of UK road traffic accidents from 2005 to 2015, sourced from the UK Department for Transport (via Kaggle, data.gov.uk), comprising over 876,497 records in yearly CSV files.
*   **Data description:** The dataset contains 32 attributes covering accident details, location, time, conditions, and severity, enabling in-depth analysis of accident factors.

### 2.2. Data Transformation

Key data transformation steps include:
*   **Handling Missing Data:** Implemented binary flags for missing spatial coordinates to retain data and track quality.
*   **Standardizing Categorical Variables:** Used binary flags for inconsistencies (e.g., empty LSOA codes) to ensure analytical transparency.
*   **Temporal Aggregation:** Converted timestamps into meaningful units (hours, days, months) for pattern identification.
*   **Spatial Clustering:** Applied clustering to geographic coordinates to identify accident hotspots.
*   **Creating Derived Variables:** Generated new metrics by analyzing relationships between existing variables.

### 2.3. Data Modeling

The data warehouse is built using a **Star Schema**:
*   **Relationship:** Dimension tables (Date, Time, LightConditions, Police, RoadType, AccidentSeverity, RoadSurfaceConditions, WeatherConditions, UrbanOrRuralArea) have a one-to-many relationship with the central `Fact_Accidents` table.
*   **Dimension tables:** Contain descriptive attributes (e.g., `Dim_Date` includes Year, Month, Day; `Dim_LightConditions` includes descriptions for light condition codes). Many dimension tables implement Slowly Changing Dimension (SCD) Type 2 to track historical changes.
*   **Fact table (`Fact_Accidents`):** Stores quantitative measures (Number_of_Vehicles, Number_of_Casualties) and foreign keys linking to dimension tables.

## CHAPTER 3. EXPERIMENTING WITH THE ETL PROCESS ON AZURE FUNCTIONS

### 3.1. ETL process on Azure Functions

The BI solution leverages Microsoft Azure for automated ETL, involving three main phases:
1.  **Ingest from local to blob storage:** Data files are automatically uploaded from local storage to Azure Blob Storage based on a schedule.
2.  **ETL data into three architectural layers (Bronze, Silver, Gold) in Azure SQL Database:** This core process uses Azure Functions for data processing, transformation, and loading, with triggers enhancing automation.
3.  **Upload data to Power BI and visualize:** Processed data from the Gold layer is loaded into Power BI for dashboard creation and insight generation.

#### 3.1.1. Az Copy to blob storage

AzCopy, a command-line tool, is used for efficient and secure transfer of data to Azure Blob Storage. PowerShell scripts embedding AzCopy commands are automated via Windows Task Scheduler for regular, unattended data uploads (e.g., daily).

#### 3.1.2. Azure Functions Deployment on Azure Portal

A FunctionApp (`RawBronzeSilverGoldlayer`) is created on Azure Portal, linked to the storage account. Three Azure Functions (`TimerBlobProcessor`, `BlobTriggerSilver`, `TimeTriggertoGold`) are developed in VS Code and deployed to this FunctionApp, each responsible for data ingestion into the Bronze, Silver, and Gold layers respectively.

### 3.2. Bronze layer data ingestion

*   **Raw-to-bronze data pipeline design:** An Azure Function automates processing yearly CSV data from Azure Blob Storage into the Bronze layer of Azure SQL Database. Blob metadata ("Processed" tag) prevents re-processing of files. CsvHelper library is used for CSV parsing, and SqlBulkCopy for efficient data insertion (full-load strategy, recreating the table each run).
*   **Schedule data updates:** A Timer Trigger on Azure Functions schedules the pipeline to run weekly (e.g., 1:00 AM every Monday) using a CRON expression, ensuring continuous data processing.
*   **Send an error report email:** SendGrid integration provides email notifications for errors or successful completion of data processing, acting as a monitoring tool.

### 3.3. Silver layer data ingestion

Data is ingested from Azure Blob Storage directly into the Silver layer. The `BlobTriggerSilver` Azure Function is activated whenever a new file is uploaded to the blob container. This function performs data transformations (standardizing date formats, adding flags for missing location data, removing records with undetermined locations) to ensure data in the Silver layer is clean, consistent, and structured. Data is bulk-inserted into a single `silver.accident1015` table.

### 3.4. Gold layer data ingestion

The `TimeTriggertoGold` Azure Function transforms data from the Silver layer's `accident1015` table into a Star Schema in the Gold layer. This involves populating multiple Dimension tables (using SCD Type 2 to preserve history) and a central `Fact_Accidents` table. The process includes extracting data, updating Dimensions, removing outdated Fact records, and loading new, linked data into the Fact table, all scheduled via a Timer Trigger. The `Fact_Accidents` table is refreshed with the latest data, ready for Power BI visualization.

### 3.5. Data Governance

A robust logging system is implemented across all three Azure Functions:
*   **Real-time logging:** `ILogger` records operational details (info, warning, error) during execution.
*   **Structured log storage:** `FunctionLog` entries are stored in a `function_logs` table in Azure SQL Database for long-term retention and analysis, capturing details like function name, status, record count, and messages.
*   **Error handling:** Detailed error information is captured in logs.
*   **Consistent Log Structure:** The `function_logs` table records both successful and failed operations from all functions.

### 3.6. Load data using Power Automate

Power Automate is used to refresh the Power BI dataset automatically.
*   A pre-built template triggers when a new item is created in the SQL Server Gold layer tables (Dim and Fact).
*   The "Add rows to a dataset in Power BI" component updates the Power BI dataset by mapping columns from SQL Database.
*   Mobile notifications provide real-time alerts on pipeline runs.

## CHAPTER 4. DATA VISUALIZATION

### 4.1. Data Modeling (in Power BI)

The data model in Power BI mirrors the Star Schema from the Gold layer.
*   `Fact_Accidents` is the central table.
*   Dimension tables have a one-to-many relationship with the fact table.
*   Custom `Dim_Date` and `Dim_Time` tables were created using M language in Power Query to enhance time-based analysis (e.g., `Dim_Date` covers 2015, `Dim_Time` provides granular hour/minute details).

### 4.2. Dashboard Visualization

Interactive dashboards in Power BI are designed to address the business questions outlined in Chapter 1. This includes:
*   **KPIs Visualization:** Tracking key metrics like Total Accidents, Fatalities, SAR over time.
*   **Page 1 - Accident Overview:** Visualizing total accident trends, distribution by severity, casualties per severity, correlation with speed limits, and accidents by road type.
*   **Page 2 - Uncontrollable Causes:** Displaying accident rates by location (map), casualty comparison between urban/rural areas, impact of weather conditions, and influence of road surface conditions.
*   **Page 3 - Controllable Causes:** Analyzing the impact of light conditions on accidents and casualties, accidents and casualties by road type, and identifying police forces handling the most accidents.


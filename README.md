
## Clinical Trial and Pharmaceutical Analysis

This project leverages Spark and Databricks for a comprehensive analysis of clinical trial datasets and pharmaceutical company records. Using Spark SQL, RDDs, and DataFrames, the project explores datasets spanning clinical trials from 2019 to 2021 and pharmaceutical violations. The analysis aims to provide critical insights into clinical trial trends, sponsors, conditions studied, and the landscape of pharmaceutical companies.

**Table of Contents**

Datasets Description
Description of Setup
Setting up Notebooks
Data Cleaning and Preparation
Question 1-5
Further Analysis
Conclusion

**Datasets Description**

The provided datasets include clinical trial records across 2019, 2020, and 2021 (Clinicaltrial_2019.csv, Clinicaltrial_2020.csv, Clinicaltrial_2021.csv) and Pharma.csv, containing pharmaceutical violations. These datasets hold crucial trial information such as sponsors, study statuses, conditions, interventions, and pharmaceutical company details.

**Description of Setup**

The analysis was conducted using Databricks, a cloud-based analytics platform, employing PySpark and Spark SQL for processing large datasets efficiently. The setup involved creating clusters and uploading datasets to the workspace for subsequent analysis.

**Setting up Notebooks**

Notebooks were configured within Databricks to work seamlessly with the datasets. Python and SQL were chosen as the primary languages for RDD, DataFrame queries, and SQL implementations.

**Data Cleaning and Preparation**

Unzipping of compressed datasets was crucial for processing. The process involved moving files to specific directories, making datasets readily available for analysis.

**Question 1-5**

Each question was systematically addressed across three implementations - RDD, DataFrame, and SQL - to derive insights and answers. These implementations covered tasks like listing study types, identifying top conditions, sponsors excluding pharmaceutical companies, completion date analysis, and further advanced features using SQL, RDD, and DataFrame methodologies.

**Usage/Examples**

Exploratory Analysis: Running the notebooks provided in this repository allows for an in-depth exploratory analysis of the clinical trial and pharmaceutical datasets. This includes visualizations, data summaries, and specific queries for insight extraction.

Custom Queries and Analysis: Users can adapt the provided queries or create new ones to derive different insights or address specific questions pertaining to clinical trials, sponsors, or pharmaceutical violations.

**Example**:
```python
clinicaltrial_Rdd.filter(lambda row: row.Conditions is not None) \
   .filter(lambda row: row.Id is not None) \
   .map(lambda row: (row.Id, row.Conditions)) \
   .flatMapValues(lambda x: x.split(",")) \
   .map(lambda row: (row[1].strip(),1)) \
   .reduceByKey(lambda a, b: a + b) \
   .map(lambda row: (row[1], row[0])) \
   .sortByKey(False) \
   .take(5)
```
**Further Analysis**
The project extended beyond initial questions, encompassing additional SQL queries, RDD and DataFrame implementations, user-defined functions, and visualization techniques using tools like Google Studio, Tableau, and Matplotlib. These provided an in-depth understanding of clinical trial landscapes, sponsor engagement, and pharmaceutical trends.

**Limitations**
Dataset Completeness: The analysis assumes the completeness and accuracy of the datasets. Any missing or incorrect information may impact the conclusions drawn.

Scalability: While Spark efficiently handles large datasets, extremely large datasets or insufficient cluster resources may affect processing times or require additional optimizations.

**Recommendations**

Data Quality Assurance: Implement robust data validation and cleansing processes to ensure dataset integrity before analysis, reducing the impact of anomalies or inaccuracies.

Resource Scaling: Monitor cluster performance and scale resources as needed to handle larger datasets or optimize processing times.

Extended Analysis: Extend the analysis to incorporate additional datasets or time frames to provide a more comprehensive view of clinical trial trends or pharmaceutical insights.


**Conclusion**

This analysis, utilizing Spark's capabilities across RDD, DataFrame, and SQL, offers critical insights into clinical trials and pharmaceutical domains. The findings serve as valuable resources for healthcare stakeholders, aiding strategic decision-making in clinical trial planning, sponsor identification, and research prioritization. The project's extended features showcase the power of advanced analytics and tools in uncovering nuanced trends within complex datasets.

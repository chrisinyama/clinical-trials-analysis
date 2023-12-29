-- Databricks notebook source
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC %python
-- MAGIC #Reading clinical trial data from a CSV file
-- MAGIC clinicaltrial_2021 = spark.read.option("header", True).option("escape",'\"').option("sep", "|").csv("/FileStore/tables/clinicaltrial_2021.csv")

-- COMMAND ----------

-- MAGIC
-- MAGIC %python
-- MAGIC #Reading pharmaceutical company data from a CSV file
-- MAGIC pharma = spark.read.option("header", True).option("escape",'\"').csv("/FileStore/tables/pharma.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating a temporary view for the pharmaceutical company data
-- MAGIC
-- MAGIC pharma.createOrReplaceTempView("Pharma")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Creating a temporary view for the clinical trial data
-- MAGIC clinicaltrial_2021.createOrReplaceTempView ("clinicaltrial_2021")

-- COMMAND ----------


--Selecting the first 5 rows from the clinical trial data--

SELECT * FROM clinicaltrial_2021 LIMIT 5

-- COMMAND ----------

--Selecting the first 5 rows from the pharmaceutical company data and visualing the output--

SELECT * FROM Pharma LIMIT 5

-- COMMAND ----------

--Describing the schema of the clinical trial data--

DESCRIBE clinicaltrial_2021

-- COMMAND ----------

--QUESTION 1
--Counting the number of distinct studies in the clinical trial data--

SELECT COUNT(DISTINCT Id) AS number_studies
FROM clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC                             --QUESTION 2--
-- MAGIC --Counting the frequency of study types in the clinical trial data and displaying the top 5 and visualization of output--
-- MAGIC
-- MAGIC SELECT Type, count(*) as frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE Type != ''
-- MAGIC GROUP BY Type
-- MAGIC ORDER BY frequency DESC
-- MAGIC LIMIT 5;

-- COMMAND ----------

---QUESTION 3--
 --Counting the frequency of conditions in the clinical trial data and displaying the top 5--
 
SELECT trim(subquery.Conditions) AS Conditions, count(*) as frequency
FROM  (SELECT explode(split(Conditions, ",")) as Conditions FROM clinicaltrial_2021) subquery
GROUP BY Conditions
ORDER BY frequency DESC
LIMIT 5;

-- COMMAND ----------

--QUESTION  4--
--selects the top 10 Sponsors who conducted the most clinical trials in 2021, but are not listed as parent companies in the pharma table. It uses a LEFT JOIN to merge data from both tables and a WHERE clause to exclude parent companies. The results are sorted by the total number of trials conducted by each sponsor.--

SELECT Sponsor, COUNT(Sponsor) AS Total_Count
FROM (
SELECT Sponsor
FROM clinicaltrial_2021
WHERE Sponsor != ''
) c
LEFT JOIN (
SELECT Parent_Company
FROM pharma
WHERE Parent_Company != ''
) p
ON c.Sponsor = p.Parent_Company
WHERE p.Parent_Company IS NULL
GROUP BY Sponsor
ORDER BY Total_Count DESC
LIMIT 10;

-- COMMAND ----------


----QUESTION 5---

-- retrieving the total number of studies completed in each month of 2021, and sorts the results by month. It manipulates the data in the Completion column using functions like from_unixtime, unix_timestamp, trim, and LEFT.--

SELECT from_unixtime(unix_timestamp(Trim(LEFT(Completion, 3)),'MMM'),'MM') 
as Months, Completion, count(*) as Total_Studies
FROM clinicaltrial_2021
WHERE Completion LIKE '%2021%'
GROUP BY Completion
ORDER BY Months;

-- COMMAND ----------



-- COMMAND ----------

--Determining the Clinical Trial Completion Dates by Month for 2021--
SELECT 
  DATE_FORMAT(CAST(UNIX_TIMESTAMP(TRIM(LEFT(Completion, 3)), 'MMM') AS TIMESTAMP), 'MM') as Months, 
  Completion, 
  COUNT(*) as Total_Studies
FROM 
  clinicaltrial_2021
WHERE 
  Completion LIKE '%2021%'
GROUP BY 
  Completion
ORDER BY 
  Months

-- COMMAND ----------

                                                   -- Further analysis--

-- COMMAND ----------

--Counting the frequency of conditions for studies sponsored by Novo Nordisk A/S and displaying the top conditions--

SELECT 
    c.Sponsor,
    c.Conditions,
    COUNT(*) AS ConditionCount
FROM 
    clinicaltrial_2021 c
WHERE 
    c.Sponsor = 'Novo Nordisk A/S'
GROUP BY 
    c.Sponsor, c.Conditions
ORDER BY 
    ConditionCount DESC


-- COMMAND ----------

--Determining the pharmaceutical company with the most clinical trials and displaying the number of trials--

WITH cte AS (
    SELECT p.Parent_Company, COUNT(*) AS Total_Trials,
           RANK() OVER (PARTITION BY p.Parent_Company ORDER BY COUNT(*) DESC) AS Rank
    FROM Pharma p
    JOIN Clinicaltrial_2021 c ON p.Company = c.Sponsor
    GROUP BY p.Parent_Company
)
SELECT Parent_Company, Total_Trials
FROM cte
WHERE Rank = 1
ORDER BY Total_Trials DESC;

-- COMMAND ----------

--Counting the number of studies per condition for completed clinical trials and grouping by sponsor, and displaying the top conditions for each sponsor--

SELECT Sponsor, Conditions, COUNT(*) as NumStudies
FROM clinicaltrial_2021
WHERE Status = 'Completed'
GROUP BY Sponsor, Conditions
ORDER BY Sponsor, NumStudies DESC;

-- COMMAND ----------

--Determining the average completion time for completed clinical trials and grouping by sponsor, and sorting by the average completion time--

SELECT Sponsor, AVG(DATEDIFF(Completion, Start)) as AvgCompletionTime
FROM clinicaltrial_2021
WHERE Status = 'Completed'
GROUP BY Sponsor
ORDER BY AvgCompletionTime ASC;


-- COMMAND ----------



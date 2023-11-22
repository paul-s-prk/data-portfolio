# Amazon Internship - SQL highlights

*I hope that although I've modified the column names and values to respect my NDA, I can provide an idea of how I utilised SQL to achieve my project aims successfully.*

During my internship at Amazon Logistics over the summer of 2022/23, I owned a project that produced an Excel dashboard for operations managers across the Last Mile (the final, and often most expensive, part of the supply chain where the parcel is delivered to the customer) Logistics Network. The dashboard provided new insights into the root cause of delivery issues, allowing for greater visibility over performance and more effective response plans. SQL was foundational to the dashboard, as I used it to extract and manipulate the data that fed into it. 

My overall project involved:
1. A scheduled SQL statement to extract relevant data from Amazon databases.
2. A Python script that read the extracted data and utilised the SQLite3 module for feature engineering and creating tables.
3. An Excel spreadsheet that utilises Power Query to take the outputs from the previous steps and visualise the data using pivot tables.

My utilisation of SQL in this project can be broken down as follows:
1. Write a scheduled PostgresSQL statement to extract data from multiple tables across databases containing millions of rows
2. Create a local SQLite database and insert data utilising Python's SQLite3 module to 
3. Conduct EDA, manipulate data and feature engineer using SQLite SQL flavour.
4. Export tables as CSV for visualisation via Microsoft Excel's Power Query and Pivot Tables/Charts.
   
## 1. PostgreSQL statement to extract data from databases
*I've chosen not to disclose my PostgreSQL statement in consideration of my NDA.*

## 2. Creating a local SQLite database using Python
This snippet shows how I set up one of the four tables in my local sqlite database. 

```Python
#import packages
import sqlite3
import pandas as pd

# create sqlite3 database
db_conn = sqlite3.connect("lastmile_performance.db")
# cursor - executes SQL code on db
c = db_conn.cursor()

# create empty tables in sqlite database
c.executescript(
    """ 
    DROP TABLE IF EXISTS late_packages;
    CREATE TABLE late_packages (    
        tracking_id                           TEXT,
        update_time                           TEXT,
        status                                TEXT,
        re_classification                     TEXT
    );                 
    """       
)         

#read csv data with pandas into a dataframe
late_packages = pd.read_csv("input/LatePackages.csv")
#load pd dataframe/table onto the empty table on the db
late_packages.to_sql('late_packages', db_conn, if_exists = 'replace', index = True)

# allows sql magic commands
%load_ext sql
%sql sqlite:///lastmile_performance.db
```

## 3. Conduct EDA, data manipulation and feature engineering using SQLite

Snippet 1. 
I created columns for the next status update time, status and reason. This made it easier for me to filter tracking_ids based on the time between updates, patterns in consecutive statuses, etc.

```SQL
SELECT
  tracking_id,
  target_delivery_date,
  DATETIME (update_time) AS update_time,
  status_update,
  reason,
  DATETIME (
    -- Adding a column for the time of
    LAG (update_time, 1) OVER (
      -- the next update
      PARTITION BY
        tracking_id,
        DATE (update_time) <= DATE (target_delivery_date)
      ORDER BY
        tracking_id,
        update_time DESC
    )
  ) AS next_update_time,
  LAG (status_update, 1) OVER (
    -- Adding a column for next status
    PARTITION BY
      tracking_id,
      DATE (update_time) <= DATE (target_delivery_date)
    ORDER BY
      tracking_id,
      update_time DESC
  ) AS next_status_update,
  LAG (reason, 1) OVER (
    -- Column for next reason
    PARTITION BY
      tracking_id,
      DATE (update_time) <= DATE (target_delivery_date)
    ORDER BY
      tracking_id,
      update_time DESC
  ) AS next_reason
FROM
  late_packages
ORDER BY
  tracking_id,
  update_time ASC
```

Snippet 2.
In this snippet, I am identifying tracking_ids that meet the criteria for category_a. I first define a CTE with the last status update of each tracking_id before being classed as a late package. The following CTE is used to identify packages that meet the criteria for category_a. Finally, I use a CASE clause to set the category column to the string 'category a' if the tracking id is found in the category_a CTE.

```SQL
DROP TABLE IF EXISTS demo_table;
CREATE TABLE demo_table AS
WITH
  -- ...
  last_status_before_miss AS (
    SELECT
      tracking_id,
      MAX(update_time) AS max_updatetime,
      status_update
    FROM
      late_package_statuses
    WHERE
      DATE (update_time) <= DATE (target_delivery_date)
    GROUP BY
      tracking_id
  ),
  category_a AS (
    SELECT
      ps.tracking_id
    FROM
      late_package_statuses AS ps
      JOIN last_status_before_miss AS ls ON ps.tracking_id = ls.tracking_id
      AND ls.status_update = 'STATUS_1'
    WHERE
      ps.status_update = 'STATUS_2'
      AND ps.reason = 'STATUS_3'
      AND ps.next_status_update = 'STATUS_1'
    UNION ALL -- utilised UNION ALL over WHERE + OR for performance 
    SELECT
      ps.tracking_id
    FROM
      late_package_statuses AS ps
      JOIN last_status_before_miss AS ls ON ps.tracking_id = ls.tracking_id
      AND ls.status_update = 'STATUS_1'
    WHERE
      ps.status_update = 'STATUS_2'
      AND ps.reason = 'STATUS_4'
      AND ps.next_status_update = 'STATUS_1'
  )
  -- ...
SELECT
  DATE (ps.target_delivery_date) AS target_delivery_date,
  ps.tracking_id,
  CASE
    -- ...
    WHEN ps.tracking_id IN (
      SELECT * FROM category_a 
    ) THEN 'category a'
    -- ...
  END AS category
FROM
  late_package_statuses AS ps
GROUP BY
  target_delivery_date,
  tracking_id
ORDER BY
  target_delivery_date,
  tracking_id;
```

Snippet 3.
In this snippet, I am also categorising missed packages, but this time, I am creating a sub-category column to break the category down even further.

```SQL
DROP TABLE IF EXISTS demo_table;
CREATE TABLE demo_table AS
WITH -- Define CTEs used to filter data into categories via JOINs
      -- the CTEs are effectively 'lists' of tracking_id which will be used to filter
  category_a AS (  -- Define a CTE with tracking_ids that meet criteria for category_a
    SELECT
      ps.tracking_id,
      ps.update_time
    FROM
      late_package_statuses AS ps
    WHERE
      ps.status_update IN ('STATUS_1') -- Condition 1
      AND ps.reason IN ('STATUS_2')  -- Condition 2
      AND DATE (ps.update_time) <= DATE (ps.target_delivery_date) -- Condition 3
  ),
  first_ind_pnov AS ( -- Define CTE to utilise data after the time it was marked a particular status
    SELECT 
      ps.tracking_id, 
      MIN(ps.update_time) AS update_time
    FROM
      late_package_statuses AS ps
      JOIN category_a AS p ON ps.tracking_id = p.tracking_id
      AND ps.status_update IN ('STATUS_3') 
      AND ps.update_time > p.update_time
    GROUP BY
      ps.tracking_id
    ORDER BY
      ps.update_time ASC
  ), 
  -- Break down data in category_a further in sub-categories
  sub_category_a AS ( -- Define CTE for sub-category a
    SELECT
      sub.tracking_id
    FROM
      late_package_statuses AS sub
      JOIN category_a AS p ON sub.tracking_id = p.tracking_id
      AND sub.status_update IN ('STATUS_4')
      AND sub.update_time > p.update_time
      JOIN first_ind_pnov AS c ON sub.tracking_id = c.tracking_id
      AND sub.update_time < c.update_time
  ),
  sub_category_b AS (
    SELECT
      sub.tracking_id
    FROM
      category_a AS sub
      JOIN first_ind_pnov AS sub1 ON sub.tracking_id = sub1.tracking_id
      LEFT JOIN sub_category_a AS sub2 ON sub.tracking_id = sub2.tracking_id
    WHERE
      sub2.tracking_id IS NULL
  ),
  sub_category_c AS (
    SELECT
      sub.tracking_id
    FROM
      category_a AS sub
      LEFT JOIN first_ind_pnov AS sub1 ON sub.tracking_id = sub1.tracking_id
      LEFT JOIN sub_category_a AS sub2 ON sub.tracking_id = sub2.tracking_id
    WHERE
      sub1.tracking_id IS NULL
      AND sub2.tracking_id IS NULL
  )
SELECT                             -- Select columns for table
  DATE (ps.target_delivery_date) AS target_delivery_date,
  ps.tracking_id,
  CASE
    WHEN ps.tracking_id IN (
      SELECT
        tracking_id
      FROM
        category_a
    ) THEN 'category a'
  END AS category,
  CASE -- Categorise tracking_ids by setting string using CTEs
    WHEN ps.tracking_id IN (
      SELECT * FROM sub_category_a
    ) THEN 'sub-category a'
    WHEN ps.tracking_id IN (
      SELECT * FROM sub_category_b
    ) THEN 'sub-category b'
    WHEN ps.tracking_id IN (
      SELECT * FROM sub_category_c
    ) THEN 'sub-category c'
  END AS sub_category
FROM
  late_package_statuses AS ps
GROUP BY
  target_delivery_date,
  tracking_id
ORDER BY
  target_delivery_date,
  tracking_id,
  update_time;
```

## 4. Export tables as csv

```Python
demo_table_csv = %sql SELECT * FROM demo_table
demo_table_csv.to_csv('dashboard_inputs/demo_table.csv')
```

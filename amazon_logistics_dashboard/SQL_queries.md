
I got a lot of SQL practice in while manipulating my tables to create new columns. I would later use these columns to allow for much greater visibility into the networks performance.
so first, much of the insights I was able to derive were from connecting the domain knowledge I gained through conversations with operations managers. I wanted to get a deeper understanding into what was driving performance at a much more granular level than what was available in order to produce ***actionable*** insights. 

My utilisation of SQL in this project can be broken down as follows:
1. Write a scheduled PostgresSQL statement to extract data from multiple tables across databases containing millions of rows
2. Create a local SQLite database and insert data utilising Python's SQLite3 module to 
3. Conduct EDA, manipulate data and feature engineer using SQLite SQL flavour.
4. Export tables as csv for visualisation via Microsoft Excel's Power Query and Pivot Tables/Charts.
   
## 1. PostgresSQL statement to extract data from databases


## 2. Creating a local SQLite database using Python


## 3. Conduct EDA, manipulate data and feature engineer using SQLite



## 4. Export tables as csv


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


..... an example of a column I made using more advanced techniques.

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
    ELSE 'Other'
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

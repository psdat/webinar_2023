WITH CTE_MY_DATE AS (
    SELECT DATEADD(DAY, SEQ4(), '2015-01-21') AS date
      FROM TABLE(GENERATOR(ROWCOUNT=>10000))  
-- Number of days after reference date in previous line
  )
  SELECT date AS date
        ,YEAR(date) AS year
        ,MONTH(date) AS month
        ,MONTHNAME(date) AS month_name
        ,DAY(date) AS day
        ,DAYOFWEEK(date) AS day_of_week
        ,WEEKOFYEAR(date) AS week_of_year
        ,DAYOFYEAR(date) AS day_of_year
    FROM CTE_MY_DATE
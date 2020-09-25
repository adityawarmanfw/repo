WITH 
first_open AS (
    SELECT DATE(TIMESTAMP_MICROS(event_timestamp),"Asia/Jakarta") AS first_open_dt, 
           user_pseudo_id
      FROM `firebase-public-project.analytics_153293282.events_*`
     WHERE _TABLE_SUFFIX BETWEEN "20180801" AND "20180930"
       AND event_name = "first_open" 
),
active as (
    SELECT DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp),"Asia/Jakarta") AS active_dt, 
                    user_pseudo_id
      FROM `firebase-public-project.analytics_153293282.events_*`
     WHERE _TABLE_SUFFIX BETWEEN "20180801" AND "20180930"
       AND event_name <> "app_remove" 
), cohort_size AS (
    SELECT first_open_dt, 
           COUNT(DISTINCT first_open.user_pseudo_id) AS users
      FROM first_open
  GROUP BY 1
)
    SELECT first_open.first_open_dt AS first_open_dt, 
           DATE_DIFF(active.active_dt, first_open.first_open_dt, DAY) AS days, 
           cohort_size.users AS cohort_users, 
           COUNT(DISTINCT active.user_pseudo_id) as retained,
           SAFE_DIVIDE(COUNT(DISTINCT active.user_pseudo_id), cohort_size.users) AS retention_rate
      FROM first_open
 LEFT JOIN cohort_size
        ON first_open.first_open_dt = cohort_size.first_open_dt
 LEFT JOIN active
        ON first_open.user_pseudo_id = active.user_pseudo_id
  GROUP BY 1,2,3 HAVING days IN (1,3,7)
  ORDER BY 1,2 ASC; 

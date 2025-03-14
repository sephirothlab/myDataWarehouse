
SELECT
  -- Combine date and time fields into ISO 8601 datetime format
  FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', 
    TIMESTAMP(
      -- Concatenate the date part (D_DATE) and time (T_HOUR, T_MINUTE, T_SECOND) into a string
      CONCAT(
        CAST(D_DATE AS STRING), 
        ' ',
        CAST(T_HOUR AS STRING), ':', 
        CAST(T_MINUTE AS STRING), ':', 
        CAST(T_SECOND AS STRING)
      )
    )
  ) AS order_datetime,
  D_DAY_NAME,
  T_SUB_SHIFT,
  so.*

FROM
  wh_staging.STORE_SALES  so
JOIN
  wh_staging.DATE_DIM dd ON so.SS_SOLD_DATE_SK = dd.D_DATE_SK  -- Replace `date_key` with the actual key
JOIN
  wh_staging.TIME_DIM td ON so.SS_SOLD_TIME_SK = td.T_TIME_SK  -- Replace `time_key` with the actual key

create or replace view `wh_conformed.CUSTOMER_DEMOGRAPHICS_VW` as
select
*,
  CASE
    WHEN CD_GENDER = 'F' THEN 'หญิง'
    WHEN CD_GENDER = 'M' THEN 'ชาย'
    ELSE 'Unknown'  -- If there are other unexpected values
  END AS CD_GENDER_TH
from `wh_raw.CUSTOMER_DEMOGRAPHICS`
create or replace view 
with top_spender as (
SELECT 
SS_CUSTOMER_SK,
SS_CDEMO_SK,
round(sum(SS_NET_PAID)) as TOTAL_AMOUNT
FROM `sephirothacademy.wh_dimension.FACT_STORE_SALES_VW`
Group By 1,2
Order By 3 desc
limit 10
)
SELECT
distinct 
ss.*,
C_SALUTATION,
C_FIRST_NAME,
C_LAST_NAME,
C_BIRTH_COUNTRY,
C_EMAIL_ADDRESS,
CD_GENDER_TH,
CD_EDUCATION_STATUS,
CD_CREDIT_RATING

FROM top_spender ss
LEFT JOIN `wh_dimension.DIM_CUSTOMER_VW` c on ss.SS_CUSTOMER_SK = c.C_CUSTOMER_SK
LEFT JOIN `wh_dimension.DIM_CUSTOMER_DEMOGRAPHICS_VW` cd on ss.SS_CDEMO_SK = cd.CD_DEMO_SK


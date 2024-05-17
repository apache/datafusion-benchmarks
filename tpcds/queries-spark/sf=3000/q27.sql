-- SQLBench-DS query 27 derived from TPC-DS query 27 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 3000.
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'S' and
       cd_education_status = 'Advanced Degree' and
       d_year = 1999 and
       s_state in ('TX','MI', 'IL', 'MN', 'NM', 'SD')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
  LIMIT 100;


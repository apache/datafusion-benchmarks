-- SQLBench-DS query 28 derived from TPC-DS query 28 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 100.
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 19 and 19+10 
             or ss_coupon_amt between 11468 and 11468+1000
             or ss_wholesale_cost between 20 and 20+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 14 and 14+10
          or ss_coupon_amt between 92 and 92+1000
          or ss_wholesale_cost between 13 and 13+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 124 and 124+10
          or ss_coupon_amt between 4023 and 4023+1000
          or ss_wholesale_cost between 50 and 50+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 74 and 74+10
          or ss_coupon_amt between 3534 and 3534+1000
          or ss_wholesale_cost between 11 and 11+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 173 and 173+10
          or ss_coupon_amt between 6017 and 6017+1000
          or ss_wholesale_cost between 45 and 45+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 2 and 2+10
          or ss_coupon_amt between 16961 and 16961+1000
          or ss_wholesale_cost between 18 and 18+20)) B6
 LIMIT 100;


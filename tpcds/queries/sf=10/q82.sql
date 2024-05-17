-- SQLBench-DS query 82 derived from TPC-DS query 82 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 10.
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 9 and 9+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-06-07' as date) and (cast('2001-06-07' as date) +  60 days)
 and i_manufact_id in (797,412,331,589)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
  LIMIT 100;


-- SQLBench-DS query 16 derived from TPC-DS query 16 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 3000.
select  
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
where
    d_date between '2000-2-01' and 
           (cast('2000-2-01' as date) + 60 days)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'AL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Dauphin County','Levy County','Luce County','Jackson County',
                  'Daviess County'
)
and exists (select *
            from catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
 LIMIT 100;


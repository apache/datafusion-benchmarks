-- SQLBench-DS query 41 derived from TPC-DS query 41 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 100.
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 762 and 762+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blush' or i_color = 'brown') and 
        (i_units = 'Bundle' or i_units = 'Unknown') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'drab' or i_color = 'purple') and
        (i_units = 'Case' or i_units = 'Tbl') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'sky' or i_color = 'linen') and
        (i_units = 'Pound' or i_units = 'Cup') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'lavender' or i_color = 'wheat') and
        (i_units = 'Carton' or i_units = 'Dozen') and
        (i_size = 'extra large' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'puff' or i_color = 'violet') and 
        (i_units = 'Pallet' or i_units = 'Bunch') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'light' or i_color = 'bisque') and
        (i_units = 'Each' or i_units = 'Gross') and
        (i_size = 'large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'mint' or i_color = 'royal') and
        (i_units = 'Ton' or i_units = 'Box') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'coral' or i_color = 'salmon') and
        (i_units = 'Tsp' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'petite')
        )))) > 0
 order by i_product_name
  LIMIT 100;


-- SQLBench-DS query 41 derived from TPC-DS query 41 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 10.
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 794 and 794+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'pink' or i_color = 'yellow') and 
        (i_units = 'Lb' or i_units = 'Pallet') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'deep' or i_color = 'goldenrod') and
        (i_units = 'Bundle' or i_units = 'Oz') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'cream') and
        (i_units = 'Case' or i_units = 'Ounce') and
        (i_size = 'medium' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'purple' or i_color = 'floral') and
        (i_units = 'Each' or i_units = 'Cup') and
        (i_size = 'small' or i_size = 'petite')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blue' or i_color = 'seashell') and 
        (i_units = 'Pound' or i_units = 'Carton') and
        (i_size = 'small' or i_size = 'petite')
        ) or
        (i_category = 'Women' and
        (i_color = 'slate' or i_color = 'saddle') and
        (i_units = 'Gram' or i_units = 'Tsp') and
        (i_size = 'extra large' or i_size = 'economy')
        ) or
        (i_category = 'Men' and
        (i_color = 'midnight' or i_color = 'chiffon') and
        (i_units = 'Box' or i_units = 'Ton') and
        (i_size = 'medium' or i_size = 'N/A')
        ) or
        (i_category = 'Men' and
        (i_color = 'orchid' or i_color = 'magenta') and
        (i_units = 'Unknown' or i_units = 'Tbl') and
        (i_size = 'small' or i_size = 'petite')
        )))) > 0
 order by i_product_name
  LIMIT 100;


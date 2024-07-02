# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import concurrent.futures
from datafusion import SessionContext
import os
import pyarrow
import subprocess
import time

table_names = [
    "call_center",
    "catalog_page",
    "catalog_sales",
    "catalog_returns",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "income_band",
    "household_demographics",
    "inventory",
    "store",
    "ship_mode",
    "reason",
    "promotion",
    "item",
    "store_sales",
    "store_returns",
    "web_page",
    "warehouse",
    "time_dim",
    "web_site",
    "web_sales",
    "web_returns",
]

all_schemas = {}

# TODO set PK/FK to not nullable

all_schemas['customer_address'] = [
    ("ca_address_sk", pyarrow.int32()),
    ("ca_address_id", pyarrow.string()),
    ("ca_street_number", pyarrow.string()),
    ("ca_street_name", pyarrow.string()),
    ("ca_street_type", pyarrow.string()),
    ("ca_suite_number", pyarrow.string()),
    ("ca_city", pyarrow.string()),
    ("ca_county", pyarrow.string()),
    ("ca_state", pyarrow.string()),
    ("ca_zip", pyarrow.string()),
    ("ca_country", pyarrow.string()),
    ("ca_gmt_offset", pyarrow.decimal128(5, 2)),
    ("ca_location_type", pyarrow.string())
]

all_schemas['customer_demographics'] = [
    ("cd_demo_sk", pyarrow.int32()),
    ("cd_gender", pyarrow.string()),
    ("cd_marital_status", pyarrow.string()),
    ("cd_education_status", pyarrow.string()),
    ("cd_purchase_estimate", pyarrow.int32()),
    ("cd_credit_rating", pyarrow.string()),
    ("cd_dep_count", pyarrow.int32()),
    ("cd_dep_employed_count", pyarrow.int32()),
    ("cd_dep_college_count", pyarrow.int32())
]

all_schemas['date_dim'] = [
    ("d_date_sk", pyarrow.int32()),
    ("d_date_id", pyarrow.string()),
    ("d_date", pyarrow.date32()),
    ("d_month_seq", pyarrow.int32()),
    ("d_week_seq", pyarrow.int32()),
    ("d_quarter_seq", pyarrow.int32()),
    ("d_year", pyarrow.int32()),
    ("d_dow", pyarrow.int32()),
    ("d_moy", pyarrow.int32()),
    ("d_dom", pyarrow.int32()),
    ("d_qoy", pyarrow.int32()),
    ("d_fy_year", pyarrow.int32()),
    ("d_fy_quarter_seq", pyarrow.int32()),
    ("d_fy_week_seq", pyarrow.int32()),
    ("d_day_name", pyarrow.string()),
    ("d_quarter_name", pyarrow.string()),
    ("d_holiday", pyarrow.string()),
    ("d_weekend", pyarrow.string()),
    ("d_following_holiday", pyarrow.string()),
    ("d_first_dom", pyarrow.int32()),
    ("d_last_dom", pyarrow.int32()),
    ("d_same_day_ly", pyarrow.int32()),
    ("d_same_day_lq", pyarrow.int32()),
    ("d_current_day", pyarrow.string()),
    ("d_current_week", pyarrow.string()),
    ("d_current_month", pyarrow.string()),
    ("d_current_quarter", pyarrow.string()),
    ("d_current_year", pyarrow.string()),
]

all_schemas["warehouse"] = [
    ("w_warehouse_sk", pyarrow.int32()),
    ("w_warehouse_id", pyarrow.string()),
    ("w_warehouse_name", pyarrow.string()),
    ("w_warehouse_sq_ft", pyarrow.int32()),
    ("w_street_number", pyarrow.string()),
    ("w_street_name", pyarrow.string()),
    ("w_street_type", pyarrow.string()),
    ("w_suite_number", pyarrow.string()),
    ("w_city", pyarrow.string()),
    ("w_county", pyarrow.string()),
    ("w_state", pyarrow.string()),
    ("w_zip", pyarrow.string()),
    ("w_country", pyarrow.string()),
    ("w_gmt_offset", pyarrow.decimal128(5, 2)),
]

all_schemas["ship_mode"] = [
    ("sm_ship_mode_sk", pyarrow.int32()),
    ("sm_ship_mode_id", pyarrow.string()),
    ("sm_type", pyarrow.string()),
    ("sm_code", pyarrow.string()),
    ("sm_carrier", pyarrow.string()),
    ("sm_contract", pyarrow.string()),
]

all_schemas["time_dim"] = [
    ("t_time_sk", pyarrow.int32()),
    ("t_time_id", pyarrow.string()),
    ("t_time", pyarrow.int32()),
    ("t_hour", pyarrow.int32()),
    ("t_minute", pyarrow.int32()),
    ("t_second", pyarrow.int32()),
    ("t_am_pm", pyarrow.string()),
    ("t_shift", pyarrow.string()),
    ("t_sub_shift", pyarrow.string()),
    ("t_meal_time", pyarrow.string()),
]

all_schemas["reason"] = [
    ("r_reason_sk", pyarrow.int32()),
    ("r_reason_id", pyarrow.string()),
    ("r_reason_desc", pyarrow.string()),
]

all_schemas["income_band"] = [
    ("ib_income_band_sk", pyarrow.int32()),
    ("ib_lower_bound", pyarrow.int32()),
    ("ib_upper_bound", pyarrow.int32()),
]


all_schemas["item"] = [
    ("i_item_sk", pyarrow.int32()),
    ("i_item_id", pyarrow.string()),
    ("i_rec_start_date", pyarrow.date32()),
    ("i_rec_end_date", pyarrow.date32()),
    ("i_item_desc", pyarrow.string()),
    ("i_current_price", pyarrow.decimal128(7, 2)),
    ("i_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("i_brand_id", pyarrow.int32()),
    ("i_brand", pyarrow.string()),
    ("i_class_id", pyarrow.int32()),
    ("i_class", pyarrow.string()),
    ("i_category_id", pyarrow.int32()),
    ("i_category", pyarrow.string()),
    ("i_manufact_id", pyarrow.int32()),
    ("i_manufact", pyarrow.string()),
    ("i_size", pyarrow.string()),
    ("i_formulation", pyarrow.string()),
    ("i_color", pyarrow.string()),
    ("i_units", pyarrow.string()),
    ("i_container", pyarrow.string()),
    ("i_manager_id", pyarrow.int32()),
    ("i_product_name", pyarrow.string()),
]

all_schemas["store"] = [
    ("s_store_sk", pyarrow.int32()),
    ("s_store_id", pyarrow.string()),
    ("s_rec_start_date", pyarrow.date32()),
    ("s_rec_end_date", pyarrow.date32()),
    ("s_closed_date_sk", pyarrow.int32()),
    ("s_store_name", pyarrow.string()),
    ("s_number_employees", pyarrow.int32()),
    ("s_floor_space", pyarrow.int32()),
    ("s_hours", pyarrow.string()),
    ("s_manager", pyarrow.string()),
    ("s_market_id", pyarrow.int32()),
    ("s_geography_class", pyarrow.string()),
    ("s_market_desc", pyarrow.string()),
    ("s_market_manager", pyarrow.string()),
    ("s_division_id", pyarrow.int32()),
    ("s_division_name", pyarrow.string()),
    ("s_company_id", pyarrow.int32()),
    ("s_company_name", pyarrow.string()),
    ("s_street_number", pyarrow.string()),
    ("s_street_name", pyarrow.string()),
    ("s_street_type", pyarrow.string()),
    ("s_suite_number", pyarrow.string()),
    ("s_city", pyarrow.string()),
    ("s_county", pyarrow.string()),
    ("s_state", pyarrow.string()),
    ("s_zip", pyarrow.string()),
    ("s_country", pyarrow.string()),
    ("s_gmt_offset", pyarrow.decimal128(5, 2)),
    ("s_tax_precentage", pyarrow.decimal128(5, 2)),
]

all_schemas["call_center"] = [
    ("cc_call_center_sk", pyarrow.int32()),
    ("cc_call_center_id", pyarrow.string()),
    ("cc_rec_start_date", pyarrow.date32()),
    ("cc_rec_end_date", pyarrow.date32()),
    ("cc_closed_date_sk", pyarrow.int32()),
    ("cc_open_date_sk", pyarrow.int32()),
    ("cc_name", pyarrow.string()),
    ("cc_class", pyarrow.string()),
    ("cc_employees", pyarrow.int32()),
    ("cc_sq_ft", pyarrow.int32()),
    ("cc_hours", pyarrow.string()),
    ("cc_manager", pyarrow.string()),
    ("cc_mkt_id", pyarrow.int32()),
    ("cc_mkt_class", pyarrow.string()),
    ("cc_mkt_desc", pyarrow.string()),
    ("cc_market_manager", pyarrow.string()),
    ("cc_division", pyarrow.int32()),
    ("cc_division_name", pyarrow.string()),
    ("cc_company", pyarrow.int32()),
    ("cc_company_name", pyarrow.string()),
    ("cc_street_number", pyarrow.string()),
    ("cc_street_name", pyarrow.string()),
    ("cc_street_type", pyarrow.string()),
    ("cc_suite_number", pyarrow.string()),
    ("cc_city", pyarrow.string()),
    ("cc_county", pyarrow.string()),
    ("cc_state", pyarrow.string()),
    ("cc_zip", pyarrow.string()),
    ("cc_country", pyarrow.string()),
    ("cc_gmt_offset", pyarrow.decimal128(5, 2)),
    ("cc_tax_percentage", pyarrow.decimal128(5, 2)),
]
 
all_schemas["customer"] = [
    ("c_customer_sk", pyarrow.int32()),
    ("c_customer_id", pyarrow.string()),
    ("c_current_cdemo_sk", pyarrow.int32()),
    ("c_current_hdemo_sk", pyarrow.int32()),
    ("c_current_addr_sk", pyarrow.int32()),
    ("c_first_shipto_date_sk", pyarrow.int32()),
    ("c_first_sales_date_sk", pyarrow.int32()),
    ("c_salutation", pyarrow.string()),
    ("c_first_name", pyarrow.string()),
    ("c_last_name", pyarrow.string()),
    ("c_preferred_cust_flag", pyarrow.string()),
    ("c_birth_day", pyarrow.int32()),
    ("c_birth_month", pyarrow.int32()),
    ("c_birth_year", pyarrow.int32()),
    ("c_birth_country", pyarrow.string()),
    ("c_login", pyarrow.string()),
    ("c_email_address", pyarrow.string()),
    ("c_last_review_date_sk", pyarrow.string()),
]
 
all_schemas["web_site"] = [
    ("web_site_sk", pyarrow.int32()),
    ("web_site_id", pyarrow.string()),
    ("web_rec_start_date", pyarrow.date32()),
    ("web_rec_end_date", pyarrow.date32()),
    ("web_name", pyarrow.string()),
    ("web_open_date_sk", pyarrow.int32()),
    ("web_close_date_sk", pyarrow.int32()),
    ("web_class", pyarrow.string()),
    ("web_manager", pyarrow.string()),
    ("web_mkt_id", pyarrow.int32()),
    ("web_mkt_class", pyarrow.string()),
    ("web_mkt_desc", pyarrow.string()),
    ("web_market_manager", pyarrow.string()),
    ("web_company_id", pyarrow.int32()),
    ("web_company_name", pyarrow.string()),
    ("web_street_number", pyarrow.string()),
    ("web_street_name", pyarrow.string()),
    ("web_street_type", pyarrow.string()),
    ("web_suite_number", pyarrow.string()),
    ("web_city", pyarrow.string()),
    ("web_county", pyarrow.string()),
    ("web_state", pyarrow.string()),
    ("web_zip", pyarrow.string()),
    ("web_country", pyarrow.string()),
    ("web_gmt_offset", pyarrow.decimal128(5, 2)),
    ("web_tax_percentage", pyarrow.decimal128(5, 2)),
]
 
all_schemas["store_returns"] = [
    ("sr_returned_date_sk", pyarrow.int32()),
    ("sr_return_time_sk", pyarrow.int32()),
    ("sr_item_sk", pyarrow.int32()),
    ("sr_customer_sk", pyarrow.int32()),
    ("sr_cdemo_sk", pyarrow.int32()),
    ("sr_hdemo_sk", pyarrow.int32()),
    ("sr_addr_sk", pyarrow.int32()),
    ("sr_store_sk", pyarrow.int32()),
    ("sr_reason_sk", pyarrow.int32()),
    ("sr_ticket_number", pyarrow.int32()),
    ("sr_return_quantity", pyarrow.int32()),
    ("sr_return_amt", pyarrow.decimal128(7, 2)),
    ("sr_return_tax", pyarrow.decimal128(7, 2)),
    ("sr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    ("sr_fee", pyarrow.decimal128(7, 2)),
    ("sr_return_ship_cost", pyarrow.decimal128(7, 2)),
    ("sr_refunded_cash", pyarrow.decimal128(7, 2)),
    ("sr_reversed_charge", pyarrow.decimal128(7, 2)),
    ("sr_store_credit", pyarrow.decimal128(7, 2)),
    ("sr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["household_demographics"] = [
    ("hd_demo_sk", pyarrow.int32()),
    ("hd_income_band_sk", pyarrow.int32()),
    ("hd_buy_potential", pyarrow.string()),
    ("hd_dep_count", pyarrow.int32()),
    ("hd_vehicle_count", pyarrow.int32()),
]
 
all_schemas["web_page"] = [
    ("wp_web_page_sk", pyarrow.int32()),
    ("wp_web_page_id", pyarrow.string()),
    ("wp_rec_start_date", pyarrow.date32()),
    ("wp_rec_end_date", pyarrow.date32()),
    ("wp_creation_date_sk", pyarrow.int32()),
    ("wp_access_date_sk", pyarrow.int32()),
    ("wp_autogen_flag", pyarrow.string()),
    ("wp_customer_sk", pyarrow.int32()),
    ("wp_url", pyarrow.string()),
    ("wp_type", pyarrow.string()),
    ("wp_char_count", pyarrow.int32()),
    ("wp_link_count", pyarrow.int32()),
    ("wp_image_count", pyarrow.int32()),
    ("wp_max_ad_count", pyarrow.int32()),
]
 
all_schemas["promotion"] = [
    ("p_promo_sk", pyarrow.int32()),
    ("p_promo_id", pyarrow.string()),
    ("p_start_date_sk", pyarrow.int32()),
    ("p_end_date_sk", pyarrow.int32()),
    ("p_item_sk", pyarrow.int32()),
    ("p_cost", pyarrow.decimal128(15, 2)),
    ("p_response_target", pyarrow.int32()),
    ("p_promo_name", pyarrow.string()),
    ("p_channel_dmail", pyarrow.string()),
    ("p_channel_email", pyarrow.string()),
    ("p_channel_catalog", pyarrow.string()),
    ("p_channel_tv", pyarrow.string()),
    ("p_channel_radio", pyarrow.string()),
    ("p_channel_press", pyarrow.string()),
    ("p_channel_event", pyarrow.string()),
    ("p_channel_demo", pyarrow.string()),
    ("p_channel_details", pyarrow.string()),
    ("p_purpose", pyarrow.string()),
    ("p_discount_active", pyarrow.string()),
]
 
all_schemas["catalog_page"] = [
    ("cp_catalog_page_sk", pyarrow.int32()),
    ("cp_catalog_page_id", pyarrow.string()),
    ("cp_start_date_sk", pyarrow.int32()),
    ("cp_end_date_sk", pyarrow.int32()),
    ("cp_department", pyarrow.string()),
    ("cp_catalog_number", pyarrow.int32()),
    ("cp_catalog_page_number", pyarrow.int32()),
    ("cp_description", pyarrow.string()),
    ("cp_type", pyarrow.string()),
]
 
all_schemas["inventory"] = [
    ("inv_date_sk", pyarrow.int32()),
    ("inv_item_sk", pyarrow.int32()),
    ("inv_warehouse_sk", pyarrow.int32()),
    ("inv_quantity_on_hand", pyarrow.int32()),
]
 
all_schemas["catalog_returns"] = [
    ("cr_returned_date_sk", pyarrow.int32()),
    ("cr_returned_time_sk", pyarrow.int32()),
    ("cr_item_sk", pyarrow.int32()),
    ("cr_refunded_customer_sk", pyarrow.int32()),
    ("cr_refunded_cdemo_sk", pyarrow.int32()),
    ("cr_refunded_hdemo_sk", pyarrow.int32()),
    ("cr_refunded_addr_sk", pyarrow.int32()),
    ("cr_returning_customer_sk", pyarrow.int32()),
    ("cr_returning_cdemo_sk", pyarrow.int32()),
    ("cr_returning_hdemo_sk", pyarrow.int32()),
    ("cr_returning_addr_sk", pyarrow.int32()),
    ("cr_call_center_sk", pyarrow.int32()),
    ("cr_catalog_page_sk", pyarrow.int32()),
    ("cr_ship_mode_sk", pyarrow.int32()),
    ("cr_warehouse_sk", pyarrow.int32()),
    ("cr_reason_sk", pyarrow.int32()),
    ("cr_order_number", pyarrow.int32()),
    ("cr_return_quantity", pyarrow.int32()),
    ("cr_return_amount", pyarrow.decimal128(7, 2)),
    ("cr_return_tax", pyarrow.decimal128(7, 2)),
    ("cr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    ("cr_fee", pyarrow.decimal128(7, 2)),
    ("cr_return_ship_cost", pyarrow.decimal128(7, 2)),
    ("cr_refunded_cash", pyarrow.decimal128(7, 2)),
    ("cr_reversed_charge", pyarrow.decimal128(7, 2)),
    ("cr_store_credit", pyarrow.decimal128(7, 2)),
    ("cr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["web_returns"] = [
    ("wr_returned_date_sk", pyarrow.int32()),
    ("wr_returned_time_sk", pyarrow.int32()),
    ("wr_item_sk", pyarrow.int32()),
    ("wr_refunded_customer_sk", pyarrow.int32()),
    ("wr_refunded_cdemo_sk", pyarrow.int32()),
    ("wr_refunded_hdemo_sk", pyarrow.int32()),
    ("wr_refunded_addr_sk", pyarrow.int32()),
    ("wr_returning_customer_sk", pyarrow.int32()),
    ("wr_returning_cdemo_sk", pyarrow.int32()),
    ("wr_returning_hdemo_sk", pyarrow.int32()),
    ("wr_returning_addr_sk", pyarrow.int32()),
    ("wr_web_page_sk", pyarrow.int32()),
    ("wr_reason_sk", pyarrow.int32()),
    ("wr_order_number", pyarrow.int32()),
    ("wr_return_quantity", pyarrow.int32()),
    ("wr_return_amt", pyarrow.decimal128(7, 2)),
    ("wr_return_tax", pyarrow.decimal128(7, 2)),
    ("wr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    ("wr_fee", pyarrow.decimal128(7, 2)),
    ("wr_return_ship_cost", pyarrow.decimal128(7, 2)),
    ("wr_refunded_cash", pyarrow.decimal128(7, 2)),
    ("wr_reversed_charge", pyarrow.decimal128(7, 2)),
    ("wr_account_credit", pyarrow.decimal128(7, 2)),
    ("wr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["web_sales"] = [
    ("ws_sold_date_sk", pyarrow.int32()),
    ("ws_sold_time_sk", pyarrow.int32()),
    ("ws_ship_date_sk", pyarrow.int32()),
    ("ws_item_sk", pyarrow.int32()),
    ("ws_bill_customer_sk", pyarrow.int32()),
    ("ws_bill_cdemo_sk", pyarrow.int32()),
    ("ws_bill_hdemo_sk", pyarrow.int32()),
    ("ws_bill_addr_sk", pyarrow.int32()),
    ("ws_ship_customer_sk", pyarrow.int32()),
    ("ws_ship_cdemo_sk", pyarrow.int32()),
    ("ws_ship_hdemo_sk", pyarrow.int32()),
    ("ws_ship_addr_sk", pyarrow.int32()),
    ("ws_web_page_sk", pyarrow.int32()),
    ("ws_web_site_sk", pyarrow.int32()),
    ("ws_ship_mode_sk", pyarrow.int32()),
    ("ws_warehouse_sk", pyarrow.int32()),
    ("ws_promo_sk", pyarrow.int32()),
    ("ws_order_number", pyarrow.int32()),
    ("ws_quantity", pyarrow.int32()),
    ("ws_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("ws_list_price", pyarrow.decimal128(7, 2)),
    ("ws_sales_price", pyarrow.decimal128(7, 2)),
    ("ws_ext_discount_amt", pyarrow.decimal128(7, 2)),
    ("ws_ext_sales_price", pyarrow.decimal128(7, 2)),
    ("ws_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("ws_ext_list_price", pyarrow.decimal128(7, 2)),
    ("ws_ext_tax", pyarrow.decimal128(7, 2)),
    ("ws_coupon_amt", pyarrow.decimal128(7, 2)),
    ("ws_ext_ship_cost", pyarrow.decimal128(7, 2)),
    ("ws_net_paid", pyarrow.decimal128(7, 2)),
    ("ws_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    ("ws_net_paid_inc_ship", pyarrow.decimal128(7, 2)),
    ("ws_net_paid_inc_ship_tax", pyarrow.decimal128(7, 2)),
    ("ws_net_profit", pyarrow.decimal128(7, 2)),
]
 
all_schemas["catalog_sales"] = [
    ("cs_sold_date_sk", pyarrow.int32()),
    ("cs_sold_time_sk", pyarrow.int32()),
    ("cs_ship_date_sk", pyarrow.int32()),
    ("cs_bill_customer_sk", pyarrow.int32()),
    ("cs_bill_cdemo_sk", pyarrow.int32()),
    ("cs_bill_hdemo_sk", pyarrow.int32()),
    ("cs_bill_addr_sk", pyarrow.int32()),
    ("cs_ship_customer_sk", pyarrow.int32()),
    ("cs_ship_cdemo_sk", pyarrow.int32()),
    ("cs_ship_hdemo_sk", pyarrow.int32()),
    ("cs_ship_addr_sk", pyarrow.int32()),
    ("cs_call_center_sk", pyarrow.int32()),
    ("cs_catalog_page_sk", pyarrow.int32()),
    ("cs_ship_mode_sk", pyarrow.int32()),
    ("cs_warehouse_sk", pyarrow.int32()),
    ("cs_item_sk", pyarrow.int32()),
    ("cs_promo_sk", pyarrow.int32()),
    ("cs_order_number", pyarrow.int32()),
    ("cs_quantity", pyarrow.int32()),
    ("cs_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("cs_list_price", pyarrow.decimal128(7, 2)),
    ("cs_sales_price", pyarrow.decimal128(7, 2)),
    ("cs_ext_discount_amt", pyarrow.decimal128(7, 2)),
    ("cs_ext_sales_price", pyarrow.decimal128(7, 2)),
    ("cs_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("cs_ext_list_price", pyarrow.decimal128(7, 2)),
    ("cs_ext_tax", pyarrow.decimal128(7, 2)),
    ("cs_coupon_amt", pyarrow.decimal128(7, 2)),
    ("cs_ext_ship_cost", pyarrow.decimal128(7, 2)),
    ("cs_net_paid", pyarrow.decimal128(7, 2)),
    ("cs_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    ("cs_net_paid_inc_ship", pyarrow.decimal128(7, 2)),
    ("cs_net_paid_inc_ship_tax", pyarrow.decimal128(7, 2)),
    ("cs_net_profit", pyarrow.decimal128(7, 2)),
]
 
all_schemas["store_sales"] = [
    ("ss_sold_date_sk", pyarrow.int32()),
    ("ss_sold_time_sk", pyarrow.int32()),
    ("ss_item_sk", pyarrow.int32()),
    ("ss_customer_sk", pyarrow.int32()),
    ("ss_cdemo_sk", pyarrow.int32()),
    ("ss_hdemo_sk", pyarrow.int32()),
    ("ss_addr_sk", pyarrow.int32()),
    ("ss_store_sk", pyarrow.int32()),
    ("ss_promo_sk", pyarrow.int32()),
    ("ss_ticket_number", pyarrow.int32()),
    ("ss_quantity", pyarrow.int32()),
    ("ss_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("ss_list_price", pyarrow.decimal128(7, 2)),
    ("ss_sales_price", pyarrow.decimal128(7, 2)),
    ("ss_ext_discount_amt", pyarrow.decimal128(7, 2)),
    ("ss_ext_sales_price", pyarrow.decimal128(7, 2)),
    ("ss_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    ("ss_ext_list_price", pyarrow.decimal128(7, 2)),
    ("ss_ext_tax", pyarrow.decimal128(7, 2)),
    ("ss_coupon_amt", pyarrow.decimal128(7, 2)),
    ("ss_net_paid", pyarrow.decimal128(7, 2)),
    ("ss_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    ("ss_net_profit", pyarrow.decimal128(7, 2)),
]

def run(cmd: str):
    print(f"Executing: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def run_and_log_output(cmd: str, log_file: str):
    print(f"Executing: {cmd}; writing output to {log_file}")
    with open(log_file, "w") as file:
        subprocess.run(cmd, shell=True, check=True, stdout=file, stderr=subprocess.STDOUT)

def convert_dat_to_parquet(ctx: SessionContext, table: str, dat_filename: str, file_extension: str, parquet_filename: str):
    print(f"Converting {dat_filename} to {parquet_filename} ...")

    # schema manipulation code copied from DataFusion Python tpcds example
    table_schema = [pyarrow.field(r[0].lower(), r[1], nullable=False) for r in all_schemas[table]]

    # Pre-collect the output columns so we can ignore the null field we add
    # in to handle the trailing | in the file
    output_cols = [r.name for r in table_schema]

    # Trailing | requires extra field for in processing
    table_schema.append(pyarrow.field("some_null", pyarrow.null(), nullable=True))

    schema = pyarrow.schema(table_schema)

    df = ctx.read_csv(dat_filename, schema=schema, has_header=False, file_extension=file_extension, delimiter="|")
    df = df.select_columns(*output_cols)
    df.write_parquet(parquet_filename, compression="snappy")

def generate_tpcds(scale_factor: int, partitions: int):
    pass

def convert_tpcds(scale_factor: int, partitions: int):
    start_time = time.time()
    ctx = SessionContext()
    if partitions == 1:
        # convert to parquet
        for table in table_names:
            convert_dat_to_parquet(ctx, table, f"data/{table}.dat", "dat", f"data/{table}.parquet")
    else:
        for table in table_names:
            run(f"mkdir -p data/{table}.parquet")
            for part in range(1, partitions + 1):
                convert_dat_to_parquet(ctx, table, f"data/{table}.dat/part-{part}.dat", "dat", f"data/{table}.parquet/part{part}.parquet")
    end_time = time.time()
    print(f"Converted CSV to Parquet in {round(end_time - start_time, 2)} seconds")

if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    subparsers = arg_parser.add_subparsers(dest='command', help='Available commands')

    parser_generate = subparsers.add_parser('generate', help='Generate TPC-DS CSV Data')
    parser_generate.add_argument('--scale-factor', type=int, help='The scale factor')
    parser_generate.add_argument('--partitions', type=int, help='The number of partitions')

    parser_convert = subparsers.add_parser('convert', help='Convert TPC-DS CSV Data to Parquet')
    parser_convert.add_argument('--scale-factor', type=int, help='The scale factor')
    parser_convert.add_argument('--partitions', type=int, help='The number of partitions')

    args = arg_parser.parse_args()
    if args.command == 'generate':
        generate_tpcds(args.scale_factor, args.partitions)
    elif args.command == 'convert':
        convert_tpcds(args.scale_factor, args.partitions)

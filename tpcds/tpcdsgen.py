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
    pyarrow.field("ca_address_sk", pyarrow.int32()),
    pyarrow.field("ca_address_id", pyarrow.string()),
    pyarrow.field("ca_street_number", pyarrow.string()),
    pyarrow.field("ca_street_name", pyarrow.string()),
    pyarrow.field("ca_street_type", pyarrow.string()),
    pyarrow.field("ca_suite_number", pyarrow.string()),
    pyarrow.field("ca_city", pyarrow.string()),
    pyarrow.field("ca_county", pyarrow.string()),
    pyarrow.field("ca_state", pyarrow.string()),
    pyarrow.field("ca_zip", pyarrow.string()),
    pyarrow.field("ca_country", pyarrow.string()),
    pyarrow.field("ca_gmt_offset", pyarrow.decimal128(5, 2)),
    pyarrow.field("ca_location_type", pyarrow.string())
]

all_schemas['customer_demographics'] = [
    pyarrow.field("cd_demo_sk", pyarrow.int32()),
    pyarrow.field("cd_gender", pyarrow.string()),
    pyarrow.field("cd_marital_status", pyarrow.string()),
    pyarrow.field("cd_education_status", pyarrow.string()),
    pyarrow.field("cd_purchase_estimate", pyarrow.int32()),
    pyarrow.field("cd_credit_rating", pyarrow.string()),
    pyarrow.field("cd_dep_count", pyarrow.int32()),
    pyarrow.field("cd_dep_employed_count", pyarrow.int32()),
    pyarrow.field("cd_dep_college_count", pyarrow.int32())
]

all_schemas['date_dim'] = [
    pyarrow.field("d_date_sk", pyarrow.int32()),
    pyarrow.field("d_date_id", pyarrow.string()),
    pyarrow.field("d_date", pyarrow.date32()),
    pyarrow.field("d_month_seq", pyarrow.int32()),
    pyarrow.field("d_week_seq", pyarrow.int32()),
    pyarrow.field("d_quarter_seq", pyarrow.int32()),
    pyarrow.field("d_year", pyarrow.int32()),
    pyarrow.field("d_dow", pyarrow.int32()),
    pyarrow.field("d_moy", pyarrow.int32()),
    pyarrow.field("d_dom", pyarrow.int32()),
    pyarrow.field("d_qoy", pyarrow.int32()),
    pyarrow.field("d_fy_year", pyarrow.int32()),
    pyarrow.field("d_fy_quarter_seq", pyarrow.int32()),
    pyarrow.field("d_fy_week_seq", pyarrow.int32()),
    pyarrow.field("d_day_name", pyarrow.string()),
    pyarrow.field("d_quarter_name", pyarrow.string()),
    pyarrow.field("d_holiday", pyarrow.string()),
    pyarrow.field("d_weekend", pyarrow.string()),
    pyarrow.field("d_following_holiday", pyarrow.string()),
    pyarrow.field("d_first_dom", pyarrow.int32()),
    pyarrow.field("d_last_dom", pyarrow.int32()),
    pyarrow.field("d_same_day_ly", pyarrow.int32()),
    pyarrow.field("d_same_day_lq", pyarrow.int32()),
    pyarrow.field("d_current_day", pyarrow.string()),
    pyarrow.field("d_current_week", pyarrow.string()),
    pyarrow.field("d_current_month", pyarrow.string()),
    pyarrow.field("d_current_quarter", pyarrow.string()),
    pyarrow.field("d_current_year", pyarrow.string()),
]

all_schemas["warehouse"] = [
    pyarrow.field("w_warehouse_sk", pyarrow.int32()),
    pyarrow.field("w_warehouse_id", pyarrow.string()),
    pyarrow.field("w_warehouse_name", pyarrow.string()),
    pyarrow.field("w_warehouse_sq_ft", pyarrow.int32()),
    pyarrow.field("w_street_number", pyarrow.string()),
    pyarrow.field("w_street_name", pyarrow.string()),
    pyarrow.field("w_street_type", pyarrow.string()),
    pyarrow.field("w_suite_number", pyarrow.string()),
    pyarrow.field("w_city", pyarrow.string()),
    pyarrow.field("w_county", pyarrow.string()),
    pyarrow.field("w_state", pyarrow.string()),
    pyarrow.field("w_zip", pyarrow.string()),
    pyarrow.field("w_country", pyarrow.string()),
    pyarrow.field("w_gmt_offset", pyarrow.decimal128(5, 2)),
]

all_schemas["ship_mode"] = [
    pyarrow.field("sm_ship_mode_sk", pyarrow.int32()),
    pyarrow.field("sm_ship_mode_id", pyarrow.string()),
    pyarrow.field("sm_type", pyarrow.string()),
    pyarrow.field("sm_code", pyarrow.string()),
    pyarrow.field("sm_carrier", pyarrow.string()),
    pyarrow.field("sm_contract", pyarrow.string()),
]

all_schemas["time_dim"] = [
    pyarrow.field("t_time_sk", pyarrow.int32()),
    pyarrow.field("t_time_id", pyarrow.string()),
    pyarrow.field("t_time", pyarrow.int32()),
    pyarrow.field("t_hour", pyarrow.int32()),
    pyarrow.field("t_minute", pyarrow.int32()),
    pyarrow.field("t_second", pyarrow.int32()),
    pyarrow.field("t_am_pm", pyarrow.string()),
    pyarrow.field("t_shift", pyarrow.string()),
    pyarrow.field("t_sub_shift", pyarrow.string()),
    pyarrow.field("t_meal_time", pyarrow.string()),
]

all_schemas["reason"] = [
    pyarrow.field("r_reason_sk", pyarrow.int32()),
    pyarrow.field("r_reason_id", pyarrow.string()),
    pyarrow.field("r_reason_desc", pyarrow.string()),
]

all_schemas["income_band"] = [
    pyarrow.field("ib_income_band_sk", pyarrow.int32()),
    pyarrow.field("ib_lower_bound", pyarrow.int32()),
    pyarrow.field("ib_upper_bound", pyarrow.int32()),
]


all_schemas["item"] = [
    pyarrow.field("i_item_sk", pyarrow.int32()),
    pyarrow.field("i_item_id", pyarrow.string()),
    pyarrow.field("i_rec_start_date", pyarrow.date32()),
    pyarrow.field("i_rec_end_date", pyarrow.date32()),
    pyarrow.field("i_item_desc", pyarrow.string()),
    pyarrow.field("i_current_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("i_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("i_brand_id", pyarrow.int32()),
    pyarrow.field("i_brand", pyarrow.string()),
    pyarrow.field("i_class_id", pyarrow.int32()),
    pyarrow.field("i_class", pyarrow.string()),
    pyarrow.field("i_category_id", pyarrow.int32()),
    pyarrow.field("i_category", pyarrow.string()),
    pyarrow.field("i_manufact_id", pyarrow.int32()),
    pyarrow.field("i_manufact", pyarrow.string()),
    pyarrow.field("i_size", pyarrow.string()),
    pyarrow.field("i_formulation", pyarrow.string()),
    pyarrow.field("i_color", pyarrow.string()),
    pyarrow.field("i_units", pyarrow.string()),
    pyarrow.field("i_container", pyarrow.string()),
    pyarrow.field("i_manager_id", pyarrow.int32()),
    pyarrow.field("i_product_name", pyarrow.string()),
]

all_schemas["store"] = [
    pyarrow.field("s_store_sk", pyarrow.int32()),
    pyarrow.field("s_store_id", pyarrow.string()),
    pyarrow.field("s_rec_start_date", pyarrow.date32()),
    pyarrow.field("s_rec_end_date", pyarrow.date32()),
    pyarrow.field("s_closed_date_sk", pyarrow.int32()),
    pyarrow.field("s_store_name", pyarrow.string()),
    pyarrow.field("s_number_employees", pyarrow.int32()),
    pyarrow.field("s_floor_space", pyarrow.int32()),
    pyarrow.field("s_hours", pyarrow.string()),
    pyarrow.field("s_manager", pyarrow.string()),
    pyarrow.field("s_market_id", pyarrow.int32()),
    pyarrow.field("s_geography_class", pyarrow.string()),
    pyarrow.field("s_market_desc", pyarrow.string()),
    pyarrow.field("s_market_manager", pyarrow.string()),
    pyarrow.field("s_division_id", pyarrow.int32()),
    pyarrow.field("s_division_name", pyarrow.string()),
    pyarrow.field("s_company_id", pyarrow.int32()),
    pyarrow.field("s_company_name", pyarrow.string()),
    pyarrow.field("s_street_number", pyarrow.string()),
    pyarrow.field("s_street_name", pyarrow.string()),
    pyarrow.field("s_street_type", pyarrow.string()),
    pyarrow.field("s_suite_number", pyarrow.string()),
    pyarrow.field("s_city", pyarrow.string()),
    pyarrow.field("s_county", pyarrow.string()),
    pyarrow.field("s_state", pyarrow.string()),
    pyarrow.field("s_zip", pyarrow.string()),
    pyarrow.field("s_country", pyarrow.string()),
    pyarrow.field("s_gmt_offset", pyarrow.decimal128(5, 2)),
    pyarrow.field("s_tax_precentage", pyarrow.decimal128(5, 2)),
]

all_schemas["call_center"] = [
    pyarrow.field("cc_call_center_sk", pyarrow.int32()),
    pyarrow.field("cc_call_center_id", pyarrow.string()),
    pyarrow.field("cc_rec_start_date", pyarrow.date32()),
    pyarrow.field("cc_rec_end_date", pyarrow.date32()),
    pyarrow.field("cc_closed_date_sk", pyarrow.int32()),
    pyarrow.field("cc_open_date_sk", pyarrow.int32()),
    pyarrow.field("cc_name", pyarrow.string()),
    pyarrow.field("cc_class", pyarrow.string()),
    pyarrow.field("cc_employees", pyarrow.int32()),
    pyarrow.field("cc_sq_ft", pyarrow.int32()),
    pyarrow.field("cc_hours", pyarrow.string()),
    pyarrow.field("cc_manager", pyarrow.string()),
    pyarrow.field("cc_mkt_id", pyarrow.int32()),
    pyarrow.field("cc_mkt_class", pyarrow.string()),
    pyarrow.field("cc_mkt_desc", pyarrow.string()),
    pyarrow.field("cc_market_manager", pyarrow.string()),
    pyarrow.field("cc_division", pyarrow.int32()),
    pyarrow.field("cc_division_name", pyarrow.string()),
    pyarrow.field("cc_company", pyarrow.int32()),
    pyarrow.field("cc_company_name", pyarrow.string()),
    pyarrow.field("cc_street_number", pyarrow.string()),
    pyarrow.field("cc_street_name", pyarrow.string()),
    pyarrow.field("cc_street_type", pyarrow.string()),
    pyarrow.field("cc_suite_number", pyarrow.string()),
    pyarrow.field("cc_city", pyarrow.string()),
    pyarrow.field("cc_county", pyarrow.string()),
    pyarrow.field("cc_state", pyarrow.string()),
    pyarrow.field("cc_zip", pyarrow.string()),
    pyarrow.field("cc_country", pyarrow.string()),
    pyarrow.field("cc_gmt_offset", pyarrow.decimal128(5, 2)),
    pyarrow.field("cc_tax_percentage", pyarrow.decimal128(5, 2)),
]
 
all_schemas["customer"] = [
    pyarrow.field("c_customer_sk", pyarrow.int32()),
    pyarrow.field("c_customer_id", pyarrow.string()),
    pyarrow.field("c_current_cdemo_sk", pyarrow.int32()),
    pyarrow.field("c_current_hdemo_sk", pyarrow.int32()),
    pyarrow.field("c_current_addr_sk", pyarrow.int32()),
    pyarrow.field("c_first_shipto_date_sk", pyarrow.int32()),
    pyarrow.field("c_first_sales_date_sk", pyarrow.int32()),
    pyarrow.field("c_salutation", pyarrow.string()),
    pyarrow.field("c_first_name", pyarrow.string()),
    pyarrow.field("c_last_name", pyarrow.string()),
    pyarrow.field("c_preferred_cust_flag", pyarrow.string()),
    pyarrow.field("c_birth_day", pyarrow.int32()),
    pyarrow.field("c_birth_month", pyarrow.int32()),
    pyarrow.field("c_birth_year", pyarrow.int32()),
    pyarrow.field("c_birth_country", pyarrow.string()),
    pyarrow.field("c_login", pyarrow.string()),
    pyarrow.field("c_email_address", pyarrow.string()),
    pyarrow.field("c_last_review_date_sk", pyarrow.string()),
]
 
all_schemas["web_site"] = [
    pyarrow.field("web_site_sk", pyarrow.int32()),
    pyarrow.field("web_site_id", pyarrow.string()),
    pyarrow.field("web_rec_start_date", pyarrow.date32()),
    pyarrow.field("web_rec_end_date", pyarrow.date32()),
    pyarrow.field("web_name", pyarrow.string()),
    pyarrow.field("web_open_date_sk", pyarrow.int32()),
    pyarrow.field("web_close_date_sk", pyarrow.int32()),
    pyarrow.field("web_class", pyarrow.string()),
    pyarrow.field("web_manager", pyarrow.string()),
    pyarrow.field("web_mkt_id", pyarrow.int32()),
    pyarrow.field("web_mkt_class", pyarrow.string()),
    pyarrow.field("web_mkt_desc", pyarrow.string()),
    pyarrow.field("web_market_manager", pyarrow.string()),
    pyarrow.field("web_company_id", pyarrow.int32()),
    pyarrow.field("web_company_name", pyarrow.string()),
    pyarrow.field("web_street_number", pyarrow.string()),
    pyarrow.field("web_street_name", pyarrow.string()),
    pyarrow.field("web_street_type", pyarrow.string()),
    pyarrow.field("web_suite_number", pyarrow.string()),
    pyarrow.field("web_city", pyarrow.string()),
    pyarrow.field("web_county", pyarrow.string()),
    pyarrow.field("web_state", pyarrow.string()),
    pyarrow.field("web_zip", pyarrow.string()),
    pyarrow.field("web_country", pyarrow.string()),
    pyarrow.field("web_gmt_offset", pyarrow.decimal128(5, 2)),
    pyarrow.field("web_tax_percentage", pyarrow.decimal128(5, 2)),
]
 
all_schemas["store_returns"] = [
    pyarrow.field("sr_returned_date_sk", pyarrow.int32()),
    pyarrow.field("sr_return_time_sk", pyarrow.int32()),
    pyarrow.field("sr_item_sk", pyarrow.int32()),
    pyarrow.field("sr_customer_sk", pyarrow.int32()),
    pyarrow.field("sr_cdemo_sk", pyarrow.int32()),
    pyarrow.field("sr_hdemo_sk", pyarrow.int32()),
    pyarrow.field("sr_addr_sk", pyarrow.int32()),
    pyarrow.field("sr_store_sk", pyarrow.int32()),
    pyarrow.field("sr_reason_sk", pyarrow.int32()),
    pyarrow.field("sr_ticket_number", pyarrow.int32()),
    pyarrow.field("sr_return_quantity", pyarrow.int32()),
    pyarrow.field("sr_return_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_return_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_fee", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_return_ship_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_refunded_cash", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_reversed_charge", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_store_credit", pyarrow.decimal128(7, 2)),
    pyarrow.field("sr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["household_demographics"] = [
    pyarrow.field("hd_demo_sk", pyarrow.int32()),
    pyarrow.field("hd_income_band_sk", pyarrow.int32()),
    pyarrow.field("hd_buy_potential", pyarrow.string()),
    pyarrow.field("hd_dep_count", pyarrow.int32()),
    pyarrow.field("hd_vehicle_count", pyarrow.int32()),
]
 
all_schemas["web_page"] = [
    pyarrow.field("wp_web_page_sk", pyarrow.int32()),
    pyarrow.field("wp_web_page_id", pyarrow.string()),
    pyarrow.field("wp_rec_start_date", pyarrow.date32()),
    pyarrow.field("wp_rec_end_date", pyarrow.date32()),
    pyarrow.field("wp_creation_date_sk", pyarrow.int32()),
    pyarrow.field("wp_access_date_sk", pyarrow.int32()),
    pyarrow.field("wp_autogen_flag", pyarrow.string()),
    pyarrow.field("wp_customer_sk", pyarrow.int32()),
    pyarrow.field("wp_url", pyarrow.string()),
    pyarrow.field("wp_type", pyarrow.string()),
    pyarrow.field("wp_char_count", pyarrow.int32()),
    pyarrow.field("wp_link_count", pyarrow.int32()),
    pyarrow.field("wp_image_count", pyarrow.int32()),
    pyarrow.field("wp_max_ad_count", pyarrow.int32()),
]
 
all_schemas["promotion"] = [
    pyarrow.field("p_promo_sk", pyarrow.int32()),
    pyarrow.field("p_promo_id", pyarrow.string()),
    pyarrow.field("p_start_date_sk", pyarrow.int32()),
    pyarrow.field("p_end_date_sk", pyarrow.int32()),
    pyarrow.field("p_item_sk", pyarrow.int32()),
    pyarrow.field("p_cost", pyarrow.decimal128(15, 2)),
    pyarrow.field("p_response_target", pyarrow.int32()),
    pyarrow.field("p_promo_name", pyarrow.string()),
    pyarrow.field("p_channel_dmail", pyarrow.string()),
    pyarrow.field("p_channel_email", pyarrow.string()),
    pyarrow.field("p_channel_catalog", pyarrow.string()),
    pyarrow.field("p_channel_tv", pyarrow.string()),
    pyarrow.field("p_channel_radio", pyarrow.string()),
    pyarrow.field("p_channel_press", pyarrow.string()),
    pyarrow.field("p_channel_event", pyarrow.string()),
    pyarrow.field("p_channel_demo", pyarrow.string()),
    pyarrow.field("p_channel_details", pyarrow.string()),
    pyarrow.field("p_purpose", pyarrow.string()),
    pyarrow.field("p_discount_active", pyarrow.string()),
]
 
all_schemas["catalog_page"] = [
    pyarrow.field("cp_catalog_page_sk", pyarrow.int32()),
    pyarrow.field("cp_catalog_page_id", pyarrow.string()),
    pyarrow.field("cp_start_date_sk", pyarrow.int32()),
    pyarrow.field("cp_end_date_sk", pyarrow.int32()),
    pyarrow.field("cp_department", pyarrow.string()),
    pyarrow.field("cp_catalog_number", pyarrow.int32()),
    pyarrow.field("cp_catalog_page_number", pyarrow.int32()),
    pyarrow.field("cp_description", pyarrow.string()),
    pyarrow.field("cp_type", pyarrow.string()),
]
 
all_schemas["inventory"] = [
    pyarrow.field("inv_date_sk", pyarrow.int32()),
    pyarrow.field("inv_item_sk", pyarrow.int32()),
    pyarrow.field("inv_warehouse_sk", pyarrow.int32()),
    pyarrow.field("inv_quantity_on_hand", pyarrow.int32()),
]
 
all_schemas["catalog_returns"] = [
    pyarrow.field("cr_returned_date_sk", pyarrow.int32()),
    pyarrow.field("cr_returned_time_sk", pyarrow.int32()),
    pyarrow.field("cr_item_sk", pyarrow.int32()),
    pyarrow.field("cr_refunded_customer_sk", pyarrow.int32()),
    pyarrow.field("cr_refunded_cdemo_sk", pyarrow.int32()),
    pyarrow.field("cr_refunded_hdemo_sk", pyarrow.int32()),
    pyarrow.field("cr_refunded_addr_sk", pyarrow.int32()),
    pyarrow.field("cr_returning_customer_sk", pyarrow.int32()),
    pyarrow.field("cr_returning_cdemo_sk", pyarrow.int32()),
    pyarrow.field("cr_returning_hdemo_sk", pyarrow.int32()),
    pyarrow.field("cr_returning_addr_sk", pyarrow.int32()),
    pyarrow.field("cr_call_center_sk", pyarrow.int32()),
    pyarrow.field("cr_catalog_page_sk", pyarrow.int32()),
    pyarrow.field("cr_ship_mode_sk", pyarrow.int32()),
    pyarrow.field("cr_warehouse_sk", pyarrow.int32()),
    pyarrow.field("cr_reason_sk", pyarrow.int32()),
    pyarrow.field("cr_order_number", pyarrow.int32()),
    pyarrow.field("cr_return_quantity", pyarrow.int32()),
    pyarrow.field("cr_return_amount", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_return_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_fee", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_return_ship_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_refunded_cash", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_reversed_charge", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_store_credit", pyarrow.decimal128(7, 2)),
    pyarrow.field("cr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["web_returns"] = [
    pyarrow.field("wr_returned_date_sk", pyarrow.int32()),
    pyarrow.field("wr_returned_time_sk", pyarrow.int32()),
    pyarrow.field("wr_item_sk", pyarrow.int32()),
    pyarrow.field("wr_refunded_customer_sk", pyarrow.int32()),
    pyarrow.field("wr_refunded_cdemo_sk", pyarrow.int32()),
    pyarrow.field("wr_refunded_hdemo_sk", pyarrow.int32()),
    pyarrow.field("wr_refunded_addr_sk", pyarrow.int32()),
    pyarrow.field("wr_returning_customer_sk", pyarrow.int32()),
    pyarrow.field("wr_returning_cdemo_sk", pyarrow.int32()),
    pyarrow.field("wr_returning_hdemo_sk", pyarrow.int32()),
    pyarrow.field("wr_returning_addr_sk", pyarrow.int32()),
    pyarrow.field("wr_web_page_sk", pyarrow.int32()),
    pyarrow.field("wr_reason_sk", pyarrow.int32()),
    pyarrow.field("wr_order_number", pyarrow.int32()),
    pyarrow.field("wr_return_quantity", pyarrow.int32()),
    pyarrow.field("wr_return_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_return_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_return_amt_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_fee", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_return_ship_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_refunded_cash", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_reversed_charge", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_account_credit", pyarrow.decimal128(7, 2)),
    pyarrow.field("wr_net_loss", pyarrow.decimal128(7, 2)),
]
 
all_schemas["web_sales"] = [
    pyarrow.field("ws_sold_date_sk", pyarrow.int32()),
    pyarrow.field("ws_sold_time_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_date_sk", pyarrow.int32()),
    pyarrow.field("ws_item_sk", pyarrow.int32()),
    pyarrow.field("ws_bill_customer_sk", pyarrow.int32()),
    pyarrow.field("ws_bill_cdemo_sk", pyarrow.int32()),
    pyarrow.field("ws_bill_hdemo_sk", pyarrow.int32()),
    pyarrow.field("ws_bill_addr_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_customer_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_cdemo_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_hdemo_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_addr_sk", pyarrow.int32()),
    pyarrow.field("ws_web_page_sk", pyarrow.int32()),
    pyarrow.field("ws_web_site_sk", pyarrow.int32()),
    pyarrow.field("ws_ship_mode_sk", pyarrow.int32()),
    pyarrow.field("ws_warehouse_sk", pyarrow.int32()),
    pyarrow.field("ws_promo_sk", pyarrow.int32()),
    pyarrow.field("ws_order_number", pyarrow.int32()),
    pyarrow.field("ws_quantity", pyarrow.int32()),
    pyarrow.field("ws_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_discount_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_coupon_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_ext_ship_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_net_paid", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_net_paid_inc_ship", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_net_paid_inc_ship_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("ws_net_profit", pyarrow.decimal128(7, 2)),
]
 
all_schemas["catalog_sales"] = [
    pyarrow.field("cs_sold_date_sk", pyarrow.int32()),
    pyarrow.field("cs_sold_time_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_date_sk", pyarrow.int32()),
    pyarrow.field("cs_bill_customer_sk", pyarrow.int32()),
    pyarrow.field("cs_bill_cdemo_sk", pyarrow.int32()),
    pyarrow.field("cs_bill_hdemo_sk", pyarrow.int32()),
    pyarrow.field("cs_bill_addr_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_customer_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_cdemo_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_hdemo_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_addr_sk", pyarrow.int32()),
    pyarrow.field("cs_call_center_sk", pyarrow.int32()),
    pyarrow.field("cs_catalog_page_sk", pyarrow.int32()),
    pyarrow.field("cs_ship_mode_sk", pyarrow.int32()),
    pyarrow.field("cs_warehouse_sk", pyarrow.int32()),
    pyarrow.field("cs_item_sk", pyarrow.int32()),
    pyarrow.field("cs_promo_sk", pyarrow.int32()),
    pyarrow.field("cs_order_number", pyarrow.int32()),
    pyarrow.field("cs_quantity", pyarrow.int32()),
    pyarrow.field("cs_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_discount_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_coupon_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_ext_ship_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_net_paid", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_net_paid_inc_ship", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_net_paid_inc_ship_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("cs_net_profit", pyarrow.decimal128(7, 2)),
]
 
all_schemas["store_sales"] = [
    pyarrow.field("ss_sold_date_sk", pyarrow.int32()),
    pyarrow.field("ss_sold_time_sk", pyarrow.int32()),
    pyarrow.field("ss_item_sk", pyarrow.int32()),
    pyarrow.field("ss_customer_sk", pyarrow.int32()),
    pyarrow.field("ss_cdemo_sk", pyarrow.int32()),
    pyarrow.field("ss_hdemo_sk", pyarrow.int32()),
    pyarrow.field("ss_addr_sk", pyarrow.int32()),
    pyarrow.field("ss_store_sk", pyarrow.int32()),
    pyarrow.field("ss_promo_sk", pyarrow.int32()),
    pyarrow.field("ss_ticket_number", pyarrow.int32()),
    pyarrow.field("ss_quantity", pyarrow.int32()),
    pyarrow.field("ss_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_ext_discount_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_ext_sales_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_ext_wholesale_cost", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_ext_list_price", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_ext_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_coupon_amt", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_net_paid", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_net_paid_inc_tax", pyarrow.decimal128(7, 2)),
    pyarrow.field("ss_net_profit", pyarrow.decimal128(7, 2)),
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

    table_schema = all_schemas[table].copy()

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
                source_file = f"data/{table}.dat/part-{part}.dat"
                if os.path.exists(source_file):
                    convert_dat_to_parquet(ctx, table, source_file, "dat", f"data/{table}.parquet/part{part}.parquet")
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
    else:
        print("invalid subcommand")

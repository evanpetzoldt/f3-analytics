import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import mysql.connector
import re
import os
from dotenv import load_dotenv

# Import secrets
dummy = load_dotenv()
database_password = os.environ.get('database_password')

# St. Charles table
stc_df = pd.read_csv('data/master_table.csv', parse_dates=['date'])

# St. Louis table
mydb = mysql.connector.connect(
  host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
  user="paxminer",
  password=database_password,
  database="f3stl"
)

q = "SELECT * FROM attendance_view"
stl_df = pd.read_sql(q, mydb)

stl_df['pax_clean'] = stl_df['PAX'].str.extract(r'(^[^\(]+)', flags=re.IGNORECASE)
stl_df['pax_clean'] = stl_df['pax_clean'].str.rstrip(' ')

stl_df['ruck_flag'] = False
stl_df.loc[stl_df['AO'] == 'rucking', 'ruck_flag'] = True

stl_df['qsource_flag'] = False
stl_df.loc[stl_df['AO'] == 'qsource', 'qsource_flag'] = True

stl_df['blackops_flag'] = False
stl_df.loc[stl_df['AO'] == 'blackops', 'blackops_flag'] = True

stl_df['bd_flag'] = ~stl_df.ruck_flag & ~stl_df.qsource_flag & ~stl_df.blackops_flag

stl_df['year_num'] = pd.to_datetime(stl_df['Date']).dt.isocalendar().year

# Aggregate
stc_agg_df = stc_df.groupby('pax')[['bd_flag', 'qsource_flag', 'blackops_flag', 'ruck_flag']].sum()
stc_agg_df.rename(columns={'bd_flag':'standard_beatdowns', 'qsource_flag':'qsource_posts', 'blackops_flag':'blackops_posts', 'ruck_flag':'rucking_posts'}, inplace=True)

stl_agg_df = stl_df[stl_df['year_num']==2021].groupby('pax_clean')[['bd_flag', 'qsource_flag', 'blackops_flag', 'ruck_flag']].sum()
stl_agg_df.rename(columns={'bd_flag':'standard_beatdowns', 'qsource_flag':'qsource_posts', 'blackops_flag':'blackops_posts', 'ruck_flag':'rucking_posts'}, inplace=True)

# Join and sum
comb_df = pd.merge(stc_agg_df, stl_agg_df, how='left', left_index=True, right_index=True, suffixes=['_stc', '_stl']).fillna(0)
comb_df['standard_beatdowns_total'] = comb_df['standard_beatdowns_stc'] + comb_df['standard_beatdowns_stl']
comb_df['qsource_posts_total'] = comb_df['qsource_posts_stc'] + comb_df['qsource_posts_stl']
comb_df['blackops_posts_total'] = comb_df['blackops_posts_stc'] + comb_df['blackops_posts_stl']
comb_df['rucking_posts_total'] = comb_df['rucking_posts_stc'] + comb_df['rucking_posts_stl']
comb_df = comb_df[[
    'standard_beatdowns_stc', 'standard_beatdowns_stl', 'standard_beatdowns_total',
    'blackops_posts_stc', 'blackops_posts_stl', 'blackops_posts_total',
    'rucking_posts_stc', 'rucking_posts_stl', 'rucking_posts_total',
    'qsource_posts_stc', 'qsource_posts_stl', 'qsource_posts_total'
]]
comb_df = comb_df.astype('int')

comb_df.to_csv('data/2021_post_counts.csv')

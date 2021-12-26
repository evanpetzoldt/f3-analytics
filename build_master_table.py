###!/mnt/nas/ml/f3-analytics/env/bin/python

from re import T
from typing import final
import mysql.connector
from mysql.connector.optionfiles import MySQLOptionsParser
from numpy.core.numeric import full, outer
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pandas.core.dtypes import dtypes
import plotly.express as px
import re
from slack import WebClient
import ssl
import os
from dotenv import load_dotenv

# Import secrets
dummy = load_dotenv()
database_password = os.environ.get('database_password')
slack_secret = os.environ.get('slack_secret')

# Inputs
home_ao_capture = date.today() + timedelta(weeks=-8) #pulls the last 8 weeks to determine home AO

# Pull source tables
mydb = mysql.connector.connect(
  host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
  user="paxminer",
  password=database_password,
  database="f3stcharles"
)

q = "SELECT * FROM backblast"
df = pd.read_sql(q, mydb)

q2 = "SELECT * FROM bd_attendance"
df2 = pd.read_sql(q2, mydb)

# instantiate Slack client
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
slack_client = WebClient(slack_secret, ssl=ssl_context)

# Pull user list
member_list = pd.DataFrame(slack_client.users_list()['members'])
member_list = member_list.drop('profile', axis=1).join(pd.DataFrame(member_list.profile.values.tolist()), rsuffix='_profile')
member_list['pax'] = member_list['display_name']
member_list.loc[member_list['display_name']=='', 'pax'] = member_list['real_name']
member_list.to_csv('data/member_list.csv', index=False)

# Pull channel list
channel_list = pd.DataFrame(slack_client.conversations_list(types='public_channel')['channels'])
channel_list.to_csv('data/channel_list.csv', index=False)

# Join channel id
# TODO: add "actual ao" for qsource, forge, blackops, etc from the "AO:" field
df = pd.merge(df, channel_list[['name', 'id']], how='left', left_on='AO', right_on='name')
df.drop(columns='name', inplace=True)
df.rename(columns={'id':'ao_id'}, inplace=True)

# Extract title and PAX / Q lists
df['backblast_title'] = df['backblast'].str.split('\n', expand=True).iloc[:,0]
df['pax_list'] = df['backblast'].str.extract(r'((?<=pax:\s|pax\s<|pax<@|pax:<|pax\s:|_pax:).*(?=\n))', flags=re.IGNORECASE)
df['q_list'] = df['backblast'].str.extract(r'((?<=Q:\s|Q\s<|Q<@|Q:<|Q\s:|_Q:).*(?=\n))', flags=re.IGNORECASE)
df['pax_q_list'] = df['pax_list'].fillna('') + df['q_list'].fillna('')
# df['ao_line'] = df['backblast'].str.extract(r'(((?<=ao:)|(?<=location:)|(?<=where:)).*(?=\n))', flags=re.IGNORECASE)[0]

# Create ruck / qsource / blackops / forge flags
df['ruck_flag'] = df['backblast_title'].str.contains(r'\b(?:pre-ruck|preruck|ruck)\b', flags=re.IGNORECASE, regex=True)
df.loc[df['AO']=='rucking', 'ruck_flag'] = True

df['qsource_flag'] = df['backblast_title'].str.contains(r'\b(?:qsource)\b', flags=re.IGNORECASE, regex=True) | \
  df['backblast_title'].str.contains(r'q[1-9]\.[1-9]', flags=re.IGNORECASE, regex=True)
df.loc[df['AO']=='qsource', 'qsource_flag'] = True

df['blackops_flag'] = df['backblast_title'].str.contains(r'\b(?:blackops)\b', flags=re.IGNORECASE, regex=True)
df.loc[df['AO']=='blackops', 'blackops_flag'] = True

df['forge_flag'] = df['backblast_title'].str.contains(r'\b(?:forge)\b', flags=re.IGNORECASE, regex=True)
df.loc[df['AO']=='ao-forge', 'forge_flag'] = True
df.loc[pd.to_datetime(df['Date']).dt.day_name()!='Wednesday', 'forge_flag'] = False

# Expand PAX list row-wise
pax_expand = df['pax_q_list'].str.extractall(r'\<@(.*?)\>', flags=re.IGNORECASE)
pax_expand.reset_index(inplace=True)
pax_expand.rename(columns={'level_0':'og_index', 0:'pax_id'}, inplace=True)
pax_expand.drop_duplicates(['og_index', 'pax_id'], inplace=True)
pax_expand.drop(columns=['match'], inplace=True)

# Expand Q list row-wise
q_expand = df['q_list'].str.extractall(r'\<@(.*?)\>', flags=re.IGNORECASE)
q_expand.reset_index(inplace=True)
q_expand.rename(columns={'level_0':'og_index', 0:'pax_id'}, inplace=True)
q_expand['q_flag'] = True
q_expand.drop_duplicates(['og_index', 'pax_id'], inplace=True)
q_expand.drop(columns=['match'], inplace=True)

# Combine PAX and Q tables
both_expand = pd.merge(pax_expand, q_expand, how='left', on=['og_index', 'pax_id'])
both_expand['q_flag'].fillna(False, inplace=True)

# Join back to primary dataset
full_expand = pd.merge(both_expand, df, how='inner', right_index=True, left_on='og_index')
full_expand.drop(['pax_list','q_list','pax_q_list','fngs','CoQ','backblast_title','Q'], axis=1, inplace=True)
full_expand.rename(columns={'AO':'ao', 'Date':'date', 'og_index':'backblast_id'}, inplace=True)
full_expand['date'] = pd.to_datetime(full_expand['date']).dt.date

# Build missing table
list_to_compare = df2.groupby(['ao_id', 'date'], as_index=False)['user_id'].count().rename(columns={'user_id':'pax_count'})
list_to_compare['Date'] = list_to_compare['date'].astype('string')
df_mod = df.groupby(['ao_id', 'Date'], as_index=False)['AO'].count()
df_mod['Date'] = df_mod['Date'].astype('string')

# Identify missing beatdowns
missing_list = pd.merge(list_to_compare, df_mod, how='outer')
missing_list = missing_list[missing_list['AO'].isna()]
missing_list['backblast_id'] = np.arange(len(missing_list)) + 100000

# Format table to match full_expand
missing_pax_list = pd.merge(df2, missing_list[['ao_id','date','pax_count','backblast_id']])
missing_pax_list['q_flag'] = missing_pax_list['user_id'] == missing_pax_list['q_user_id']
missing_pax_list.rename(columns={'user_id':'pax_id'}, inplace=True)
missing_pax_list.drop(['q_user_id'], axis=1, inplace=True)
missing_pax_list['date'] = pd.to_datetime(missing_pax_list['date']).dt.date
missing_pax_list['fng_count'] = 0
missing_pax_list['backblast'] = None
missing_pax_list['ruck_flag'] = False
missing_pax_list['qsource_flag'] = False
missing_pax_list['blackops_flag'] = False
missing_pax_list['forge_flag'] = False
missing_pax_list = pd.merge(missing_pax_list, channel_list[['id', 'name']], how='left', left_on='ao_id', right_on='id').rename(columns={'name':'ao'}).drop('id', axis=1)

# Build final master table and export
final_df = pd.concat([full_expand, missing_pax_list], ignore_index=True)
final_df = pd.merge(final_df, member_list[['id', 'pax']], how='left', left_on='pax_id', right_on='id').drop('id', axis=1)
final_df = final_df[~final_df['pax'].isna()]

final_df['month_name'] = pd.DatetimeIndex(final_df['date']).month_name()
final_df['day_of_week'] = pd.DatetimeIndex(final_df['date']).day_name()
final_df['year_num'] = pd.DatetimeIndex(final_df['date']).year
final_df['week_num'] = pd.Int64Index(pd.DatetimeIndex(final_df['date']).isocalendar().week)
final_df['day_num'] = pd.DatetimeIndex(final_df['date']).day

# More stuff
final_df['bd_flag'] = ~final_df.blackops_flag & ~final_df.qsource_flag & ~final_df.ruck_flag & ~(final_df.ao.isin(['1st-f', '2nd-f', '3rd-f']))

# Pull most postings in last ~month by PAX
home_ao_df = final_df[final_df['date'] > home_ao_capture].groupby(['pax', 'ao'], as_index=False)['day_num'].count()
home_ao_df = home_ao_df[home_ao_df['ao'].str.contains('^ao')] # this prevents home AO being assigned to blackops, rucking, etc... could be changed in the future
home_ao_df.sort_values(['pax','day_num'], ascending=False, inplace=True)
home_ao_df = home_ao_df.groupby(['pax'], as_index=False)['ao'].first()
home_ao_df.rename(columns={'ao':'home_ao'}, inplace=True)

# Merge home AO and Site Q
final_df = pd.merge(final_df, home_ao_df, how='left')
final_df['home_ao'].fillna('unknown', inplace=True)

# Export final table
final_df.to_csv('data/master_table.csv', index=False)

# TODO: add reasonability tests
# The following combos shouldn't happen
# final_df[(final_df.qsource_flag & final_df.blackops_flag)]
# final_df[((final_df.ruck_flag | final_df.blackops_flag | final_df.qsource_flag) & final_df.forge_flag)]

# beatdown_view = final_df[(~final_df.qsource_flag & ~final_df.ruck_flag & ~final_df.blackops_flag)].groupby('backblast_id', as_index=False)[['year_num', 'week_num']].max()
# beatdown_view.groupby(['year_num', 'week_num'])['backblast_id'].count()
from re import T
from typing import final
import mysql.connector
from mysql.connector.optionfiles import MySQLOptionsParser
from numpy.core.numeric import full, outer
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
import re
from slack import WebClient
import ssl
import os
from dotenv import load_dotenv

# Import secrets
dummy = load_dotenv()
database_password = os.environ.get('database_password')
database_write_password = os.environ.get('database_write_password')
slack_secret = os.environ.get('slack_secret')

# mydb = mysql.connector.connect(
#   host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
#   user="f3stcharles",
#   password=database_write_password,
#   database="f3stcharles"
# )
# q = "SELECT * FROM aos"
# df = pd.read_sql(q, mydb)
# mycursor = mydb.cursor()
# mycursor.execute("UPDATE aos SET backblast = 1 WHERE ao = 'achievement-unlocked';")
# mycursor.execute("COMMIT;")

mydb = mysql.connector.connect(
  host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
  user="paxminer",
  password=database_password,
  database="f3stcharles"
)

# print(mydb)

mycursor = mydb.cursor()
# mycursor.execute("SHOW DATABASES")
mycursor.execute("SHOW TABLES")
# mycursor.execute("SELECT * FROM beatdowns LIMIT 5")

for x in mycursor:
  print(x)

# q = "SELECT * FROM attendance_view LIMIT 5"
# q = "SELECT DISTINCT AO FROM attendance_view"
# q = "SELECT * FROM backblast LIMIT 5"
q2 = "SELECT * FROM attendance_view"
q = "SELECT * FROM backblast"
q3 = "SELECT * FROM bd_attendance"
# q = "SELECT * FROM users LIMIT 5"
df = pd.read_sql(q, mydb)
df2 = pd.read_sql(q2, mydb)
df3 = pd.read_sql(q3, mydb)

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
df = pd.merge(df, channel_list[['name', 'id']], how='left', left_on='AO', right_on='name')
df.drop(columns='name', inplace=True)
df.rename(columns={'id':'ao_id'}, inplace=True)

# Extract title and PAX / Q lists
df['backblast_title'] = df['backblast'].str.split('\n', expand=True).iloc[:,0]
df['pax_list'] = df['backblast'].str.extract(r'((?<=pax:\s|pax\s<|pax<@|pax:<|pax\s:|_pax:).*(?=\n))', flags=re.IGNORECASE)
df['q_list'] = df['backblast'].str.extract(r'((?<=Q:\s|Q\s<|Q<@|Q:<|Q\s:|_Q:).*(?=\n))', flags=re.IGNORECASE)
df['pax_q_list'] = df['pax_list'].fillna('') + df['q_list'].fillna('')

# Create ruck / qsource / blackops / forge flags
df['ruck_flag'] = df['backblast_title'].str.contains(r'\b(?:pre-ruck|preruck|ruck)\b', flags=re.IGNORECASE, regex=True)
df.loc[df['AO']=='rucking', 'ruck_flag'] = True

df['qsource_flag'] = df['backblast_title'].str.contains(r'\b(?:qsource)\b', flags=re.IGNORECASE, regex=True)
# TODO: add regex for "Q#.#" in title
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
full_expand.drop(['og_index','pax_list','q_list','pax_q_list','fngs','CoQ','backblast_title','Q'], axis=1, inplace=True)
full_expand.rename(columns={'AO':'ao', 'Date':'date'}, inplace=True)
full_expand['date'] = pd.to_datetime(full_expand['date'])

# Build missing table
list_to_compare = df3.groupby(['ao_id', 'date'], as_index=False)['user_id'].count().rename(columns={'user_id':'pax_count'})
list_to_compare['Date'] = list_to_compare['date'].astype('string')
df_mod = df.groupby(['ao_id', 'Date'], as_index=False)['AO'].count()
df_mod['Date'] = df_mod['Date'].astype('string')

# Identify missing beatdowns
missing_list = pd.merge(list_to_compare, df_mod, how='outer')
missing_list = missing_list[missing_list['AO'].isna()]

# Format table to match full_expand
missing_pax_list = pd.merge(df3, missing_list[['ao_id','date','pax_count']])
missing_pax_list['q_flag'] = missing_pax_list['user_id'] == missing_pax_list['q_user_id']
missing_pax_list.rename(columns={'user_id':'pax_id'}, inplace=True)
missing_pax_list.drop(['q_user_id'], axis=1, inplace=True)
missing_pax_list['date'] = pd.to_datetime(missing_pax_list['date'])
missing_pax_list['fng_count'] = 0
missing_pax_list['backblast'] = None
missing_pax_list['ruck_flag'] = False
missing_pax_list['qsource_flag'] = False
missing_pax_list['blackops_flag'] = False
missing_pax_list['forge_flag'] = False
missing_pax_list = pd.merge(missing_pax_list, channel_list[['id', 'name']], how='left', left_on='ao_id', right_on='id').rename(columns={'name':'ao'}).drop('id', axis=1)

# Build final master table and export
# TODO: add unique beatdown identifier
final_df = pd.concat([full_expand, missing_pax_list], ignore_index=True)
final_df = pd.merge(final_df, member_list[['id', 'pax']], how='left', left_on='pax_id', right_on='id').drop('id', axis=1)
final_df = final_df[~final_df['pax'].isna()]

final_df['month_name'] = final_df['date'].dt.month_name()
final_df['day_of_week'] = final_df['date'].dt.day_name()
final_df['year_num'] = final_df['date'].dt.isocalendar().year
final_df['week_num'] = final_df['date'].dt.isocalendar().week
final_df['day_num'] = final_df['date'].dt.isocalendar().day

final_df.to_csv('data/master_table.csv', index=False)




test1 = full_expand.groupby(['Date', 'AO'], as_index=False)['pax_id'].count()
test1.rename(columns={'pax_id':'PAX_ecp'}, inplace=True)
test1['Date'] = test1['Date'].astype('string')
test2 = df2.groupby(['Date', 'AO'], as_index=False)['PAX'].count()

test3 = pd.merge(test1, test2, how='right')

df_filter = df[(df.ruck_flag | df.qsource_flag | df.blackops_flag)]

test = df.groupby(['AO','Date','Q','CoQ','pax_count','fng_count'], as_index=False)['AO'].count()

test = df['backblast'][0]

for i in range(50):
  print(test[i]=='\n')

temp = re.search(r'.+?(?=\n)', test)
df['backblast'].str.split('\n', expand=True).iloc[:,0]

q = "SELECT AO, COUNT(*) AS COUNTS, MAX(Date) AS Max_Date, MIN(Date) AS Min_Date FROM attendance_view WHERE PAX = 'Heisenberg ' AND AO <> 'rucking' AND AO <> 'qsource' GROUP BY AO"
q = "SELECT COUNT(*) AS COUNTS FROM attendance_view WHERE PAX = 'Heisenberg (F3 St. Charles)' AND AO <> 'rucking' AND AO <> 'qsource'"

q2 = "SELECT * FROM attendance_view"
df2 = pd.read_sql(q2, mydb)
# df

df['day_of_week'] = pd.to_datetime(df['Date']).dt.day_name()
df['week_num'] = pd.to_datetime(df['Date']).dt.isocalendar().week
df['day_num'] = pd.to_datetime(df['Date']).dt.isocalendar().day

df_ao_week = df.groupby(['AO','week_num'], as_index=False)['pax_count'].mean()

df2 = pd.merge(df, df_ao_week, validate='many_to_one', on=['AO', 'week_num'], suffixes=['','_ao_week_avg'])
df2['day_adj_pax'] = df2['pax_count'] / df2['pax_count_ao_week_avg']

df_filter = df[df['AO'].isin(['ao-braveheart', 'ao-eagles-nest', 'ao-field-of-dreams', 'ao-forge', 'ao-running-with-animals', 'ao-the-citadel', 'ao-the-last-stop'])]
df_agg = df_filter.groupby(['day_num', 'day_of_week'], as_index=False)['pax_count'].mean()
temp = df_filter.groupby(['AO', 'week_num'], as_index=False).agg({'day_num':np.count_nonzero, 'pax_count':sum})
df[(df['AO']=='ao-the-last-stop') & (df['week_num']==38)]

df_filter2 = df2[df2['AO'].isin(['ao-braveheart', 'ao-eagles-nest', 'ao-field-of-dreams', 'ao-forge', 'ao-running-with-animals', 'ao-the-citadel', 'ao-the-last-stop'])]
df_agg2 = df_filter2.groupby(['day_num', 'day_of_week'], as_index=False)['day_adj_pax'].mean()

df_agg
df_agg2[df_agg2['day_num'] < 7][['day_of_week', 'day_adj_pax']]


mydb = mysql.connector.connect(
  host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
  user="paxminer",
  password=database_password,
  database="f3stl"
)

df = pd.read_sql(q, mydb)

df['day_of_week'] = pd.to_datetime(df['Date']).dt.day_name()
df['year_num'] = pd.to_datetime(df['Date']).dt.isocalendar().year
df['week_num'] = pd.to_datetime(df['Date']).dt.isocalendar().week
df['day_num'] = pd.to_datetime(df['Date']).dt.isocalendar().day

df_ao_week = df.groupby(['AO','year_num','week_num'], as_index=False)['pax_count'].mean()

df2 = pd.merge(df, df_ao_week, validate='many_to_one', on=['AO', 'year_num', 'week_num'], suffixes=['','_ao_week_avg'])
df2['day_adj_pax'] = df2['pax_count'] / df2['pax_count_ao_week_avg']

df_filter2 = df2[df2['AO'].str.contains('^ao')]
df_agg2 = df_filter2.groupby(['day_num', 'day_of_week'], as_index=False)['day_adj_pax'].mean()

df_agg2[df_agg2['day_num'] < 7][['day_of_week', 'day_adj_pax']]

#%%
# Show all databases
mydb = mysql.connector.connect(
  host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
  user="paxminer",
  password=database_password
)

mycursor = mydb.cursor()
mycursor.execute("SHOW DATABASES")

region_list = []
for x in mycursor:
  if str.startswith(x[0], 'f3'):
    region_list.append(x[0])

#%%
# Attempt combined dataset
q = "SELECT * FROM beatdown_info"
i = 0
df_temp = {}

for region in region_list:
  print('Pulling ' + region)
  # initialize db connection
  mydb = mysql.connector.connect(
    host="f3stlouis.cac36jsyb5ss.us-east-2.rds.amazonaws.com",
    user="paxminer",
    password=database_password,
    database=region
  )

  # Pull all from beatdown_info
  df_temp[i] = pd.read_sql(q, mydb)
  df_temp[i]['region'] = region
  i += 1

#%%
# Combine region dfs
df_all = pd.concat(df_temp, ignore_index=True)

#%%
# Pull min and max dates for each region
df_all_region = df_all.groupby(['region'], as_index=False).agg(
  {'Date': [min, max, np.count_nonzero]}
)
df_all_region.columns = df_all_region.columns.map('_'.join).str.strip('_')

#%% 
# Pull min and max dates for each AO
df_all_ao = df_all.groupby(['region','AO'], as_index=False).agg(
  {'Date': [min, max, np.count_nonzero]}
)
df_all_ao.columns = df_all_ao.columns.map('_'.join).str.strip('_')

#%%
# Filter erroneous dates (later we can fix them)
min_allowable_date = date(2020, 1, 1)
max_allowable_date = date(2021, 12, 31)
df_all = df_all[(df_all['Date'] <= max_allowable_date) & (df_all['Date'] >= min_allowable_date)]

#%%
# Run date transforms
df_all['month_name'] = pd.to_datetime(df_all['Date']).dt.month_name()
df_all['day_of_week'] = pd.to_datetime(df_all['Date']).dt.day_name()
df_all['year_num'] = pd.to_datetime(df_all['Date']).dt.isocalendar().year
df_all['week_num'] = pd.to_datetime(df_all['Date']).dt.isocalendar().week
df_all['day_num'] = pd.to_datetime(df_all['Date']).dt.isocalendar().day

#%%
# Day of week summary for St Charles
# Filter to St Charles, and primary AOs only
df = df_all[df_all['region'] == 'f3stl']
df = df[df['AO'].str.contains('^ao')]

# Calculate avg PAX count by AO / week
df_ao_week = df.groupby(['AO','year_num','week_num'], as_index=False)['pax_count'].mean()
df_region_week = df.groupby(['year_num','week_num'], as_index=False)['pax_count'].mean()

# Merge average PAX count back on
df2 = pd.merge(df, df_ao_week, validate='many_to_one', on=['AO', 'year_num', 'week_num'], suffixes=['','_ao_week_avg'])
df2 = pd.merge(df2, df_region_week, validate='many_to_one', on=['year_num', 'week_num'], suffixes=['','_region_week_avg'])

# Calculate day of week relativity
df2['day_rel_actual'] = df2['pax_count'] / df2['pax_count_region_week_avg'] - 1
df2['day_rel_adj'] = df2['pax_count'] / df2['pax_count_ao_week_avg'] - 1

#%%
# Aggregate results
df_agg2 = df2.groupby(['day_num', 'day_of_week'], as_index=False)['day_rel_actual', 'day_rel_adj'].mean()
df_agg2 = df_agg2[df_agg2['day_of_week'] != 'Sunday'].drop(columns=['day_num'])

#%%
# Plot results
fig = px.line(
    df_agg2,
    x='day_of_week',
    y=['day_rel_actual', 'day_rel_adj'],
    title='PAX Posting Rates by Day of Week'
)
fig.update_layout(template='plotly_dark')
fig.update_layout(yaxis=dict(tickformat='.0%'))
fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-0.3,
    xanchor="center",
    x=.5
))
fig.update_layout(
    yaxis_title="Posting Rate",
    legend_title=""
)
fig.show()

#%%
# Raw all-region count calcs

# Filter to St Charles, and primary AOs only
df = df_all[df_all['region'] == 'f3stcharles']
df = df[df['AO'].str.contains('^ao')]

# Aggregate to Region / Daily level
df3 = df.groupby(['year_num', 'week_num', 'day_num', 'day_of_week'], as_index=False)['pax_count', 'fng_count'].sum()

# Aggregate to Region / Weekly level and calculate averages
df_region_week = df3.groupby(['year_num', 'week_num'])['pax_count', 'fng_count'].mean()

# Join back to df3
df4 = pd.merge(df3, df_region_week, validate='many_to_one', on=['year_num', 'week_num'], suffixes=['', '_region_week_avg'])

# Calculate day of week relativity
df4['day_pax_rel'] = df4['pax_count'] / df4['pax_count_region_week_avg'] - 1
df4['day_fng_rel'] = df4['fng_count'] / df4['fng_count_region_week_avg'] - 1

#%%
# Aggregate region count calcs
df_agg4 = df4.groupby(['day_num', 'day_of_week'], as_index=False)['day_pax_rel', 'day_fng_rel'].mean()
df_agg4 = df_agg4[df_agg4['day_of_week'] != 'Sunday'].drop(columns=['day_num'])

#%%
fig = px.line(
    df_agg4,
    x='day_of_week',
    y=['day_pax_rel', 'day_fng_rel'],
    title='PAX Posting by Day of Week - St Charles Region'
)
fig.update_layout(template='plotly_dark')
fig.update_layout(yaxis=dict(tickformat='.0%'))
fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-0.3,
    xanchor="center",
    x=.5
))
fig.update_layout(
    yaxis_title="Posting Relativity",
    legend_title=""
)
fig.show()

#%%
# Weekly Counts

# Filter to St Charles, and primary AOs only
df = df_all[df_all['region'] == 'f3stcharles']
df = df[df['AO'].str.contains('^ao')]

# Aggregate to Region / Daily level
df5 = df.groupby(['year_num', 'week_num'], as_index=False).agg({
  'Date':min, 'pax_count':sum, 'fng_count':sum
})

#%%
fig = px.line(
    df5,
    x='Date',
    y=['pax_count'],
    title='PAX Posts by Week - St Charles Region'
)
fig.update_layout(template='plotly_dark')
# fig.update_layout(yaxis=dict(tickformat='.0%'))
fig.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-0.4,
    xanchor="center",
    x=.5
))
fig.update_layout(
    yaxis_title="PAX Posts",
    xaxis_title="Week Beginning",
    legend_title=""
)
fig.show()
#%%
mydb.close()

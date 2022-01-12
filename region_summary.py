from re import T
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Import master table
df = pd.read_csv('data/master_table.csv', parse_dates=['date'])
df.loc[(df['week_num'] == 52) & (df['month_num'] == 1),'year_num'] += -1

# Filter to St Charles, non qsource, non ruck
df = df[df.bd_flag | df.blackops_flag]

# Aggregate to region / weekly / PAX level
df2 = df.groupby(['year_num', 'week_num', 'pax'], as_index=False).agg(
    {'pax':np.count_nonzero, 'date':min}
)

# Roll up to region / weekly level
df3 = df2.groupby(['year_num', 'week_num'], as_index=False).agg(
    {'pax':[sum, np.count_nonzero], 'date':min, }
)
df3.columns = df3.columns.map('_'.join).str.strip('_')

# Summarize beatdown data
df4 = df.groupby(['backblast_id'], as_index=False)[['date', 'year_num', 'week_num', 'ao', 'pax_count', 'fng_count']].max()

# Aggregate to region / weekly level
df5 = df4.groupby(['year_num', 'week_num'], as_index=False).agg(
    {'fng_count':sum, 'pax_count':sum, 'ao':np.count_nonzero}
)

# Join fng and pax_counts
df6 = pd.merge(df3, df5, validate='one_to_one', on=['year_num', 'week_num'])

# Drop unneeded columns, rename, and filter
df6.drop(columns=['year_num', 'week_num', 'pax_sum'], axis=1, inplace=True)
df6.rename(columns={'pax_count_nonzero':'unique_posters', 'date_min':'week_start', 'ao':'beatdown_count'}, inplace=True)
df6 = df6[(df6['week_start'] >= str(date(2021, 8, 2))) & (df6['week_start'] <= str(date(2022, 1, 9)))]

# Added calcs
df6['unique_posters'] = df6['unique_posters'] + df6['fng_count']
df6['avg_posts_per_beatdown'] = df6['pax_count'] / df6['beatdown_count']
df6['avg_posts_per_pax'] = df6['pax_count'] / df6['unique_posters']

# Summarize QSource
df7 = pd.read_csv('data/master_table.csv', parse_dates=['date'])
df7 = df7[(df7.qsource_flag)]
df7 = df7.groupby(['backblast_id'], as_index=False)[['date', 'year_num', 'week_num', 'ao', 'pax_count', 'fng_count']].max()

df8 = df7.groupby(['year_num', 'week_num'], as_index=False).agg(
    {'date':min, 'pax_count':sum}
)
df8 = df8[(df8['date'] >= datetime(2021, 8, 2)) & (df8['date'] <= datetime(2022, 1, 9))]

# Multiplot
fig = make_subplots(rows=4, cols=2, subplot_titles=('Total Weekly Posts', 'Avg. PAX per Beatdown', 'Weekly Active PAX Count', 'Avg. Posts Per Active PAX', 'FNGs', 'QSource Posts', 'Recorded Beatdowns'))
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['pax_count']), row=1, col=1)
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['avg_posts_per_beatdown']), row=1, col=2)
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['unique_posters']), row=2, col=1)
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['avg_posts_per_pax']), row=2, col=2)
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['fng_count']), row=3, col=1)
fig.add_trace(go.Scatter(x=df8['date'], y=df8['pax_count']), row=3, col=2)
fig.add_trace(go.Scatter(x=df6['week_start'], y=df6['beatdown_count']), row=4, col=1)
fig.update_layout(template='plotly_dark', showlegend=False, title_text='St. Charles Weekly Postings', height=800, width=1000)
fig.show()

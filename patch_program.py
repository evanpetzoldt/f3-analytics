from re import T, template
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# Import master table
df = pd.read_csv('data/master_table.csv', parse_dates=['date'])
df = df.loc[df['date']<=datetime(2021, 11, 30)]

# TODO: add flags for the Fs (1st 2nd 3rd) or just "all other"?
# TODO: make some adjustment for forge counting as an extra location?
pax_view = df.groupby(['pax', 'pax_id'], as_index=False)[['bd_flag', 'q_flag', 'forge_flag', 'qsource_flag', 'ruck_flag', 'blackops_flag']].sum()

# PAX AO Bonus
pax_week_ao_view = df[df.bd_flag].drop_duplicates(['year_num', 'week_num', 'pax_id', 'ao_id'])
pax_week_view = pax_week_ao_view.groupby(['year_num', 'week_num', 'pax_id'], as_index=False)['bd_flag'].count().rename(columns={'bd_flag':'extra_aos_hit_week'})
pax_week_view['extra_aos_hit_week'] = pax_week_view['extra_aos_hit_week'] - 1
pax_ao_view = pax_week_view.groupby('pax_id', as_index=False)['extra_aos_hit_week'].sum()
pax_view = pd.merge(pax_view, pax_ao_view, how='left').fillna(0)

# Point system
points_beatdown = 1
points_q = 1
points_forge = 1
points_qsource = 3
points_ruck = 0
points_blackops = 0
points_extra_ao = 1

# Points calc
pax_view['points'] = \
    pax_view.bd_flag * points_beatdown + \
    pax_view.q_flag * pax_view.bd_flag * points_q + \
    pax_view.forge_flag * points_forge + \
    pax_view.qsource_flag * points_qsource + \
    pax_view.ruck_flag * points_ruck + \
    pax_view.blackops_flag * points_blackops + \
    pax_view.extra_aos_hit_week * points_extra_ao

# TODO: make this proration dynamic based on pax start date
pax_view['points_adj'] = pax_view['points'] * 3

# Histogram
fig = px.histogram(pax_view[pax_view.points > 10], x='points_adj', nbins=10) #, histnorm='percent')
fig.update_layout(bargap=0.2, template='plotly_dark')
fig.show()

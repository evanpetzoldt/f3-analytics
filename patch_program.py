###!/mnt/nas/ml/f3-analytics/env/bin/python

from re import T, template
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from functools import reduce
import ssl
from slack_sdk import WebClient
import os
from dotenv import load_dotenv

# Inputs
year_select = 2022
achievement_channel = 'C02SB7S6ZDL'

# Import secrets
dummy = load_dotenv()
slack_secret = os.environ.get('slack_secret')

# Import master table
df = pd.read_csv('data/master_table.csv', parse_dates=['date'])
df = df.loc[df['year_num']==year_select] # Filter to year of interest

# Change q_flag definition to only include qs for beatdowns
# This will affect both points AND achievements
df.q_flag = df.q_flag * df.bd_flag 

# Periodic aggregations - weekly, monthly, yearly
# "view" tables are aggregated at the level they are calculated, "agg" tables aggregate them to the annual / pax level for joining together
# These aggregations are used for both the points and achievements

#####################################
#           Weekly views            #
#####################################
# Beatdowns only, ao / week level
pax_week_ao_view = df[df.bd_flag].groupby(
    ['week_num', 'ao_id', 'pax_id']
    )[['bd_flag', 'q_flag']].sum().rename(
    columns={
        'bd_flag':'bd', 'q_flag':'q'
    }
)
# Week level
pax_week_view = pax_week_ao_view.groupby(
    ['week_num', 'pax_id'])[['bd', 'q']].agg(
    ['sum', np.count_nonzero])
pax_week_view.columns = pax_week_view.columns.map('_'.join).str.strip('_')
pax_week_view.rename(
    columns={
        'bd_sum':'bd_sum_week', 'q_sum':'q_sum_week', 
        'bd_count_nonzero':'bd_ao_count_week', 'q_count_nonzero':'q_ao_count_week'
    },
    inplace=True
)
# Aggregate to pax level
pax_week_agg = pax_week_view.groupby(
    ['pax_id']
    ).max().rename(
        columns={
            'bd_sum_week':'bd_sum_week_max', 'q_sum_week':'q_sum_week_max',
            'bd_ao_count_week':'bd_ao_count_week_max', 'q_ao_count_week':'q_ao_count_week_max'
        }
)
# Special counts for travel bonus
pax_week_view2 = pax_week_view
pax_week_view2['bd_ao_count_week_extra'] = pax_week_view2['bd_ao_count_week'] - 1
pax_week_agg2 = pax_week_view.groupby(
    ['pax_id']
    )[['bd_ao_count_week_extra']].sum().rename(
        columns={
            'bd_ao_count_week_extra':'bd_ao_count_week_extra_year'
        }
)

# QSources (only once/week counts for points)
pax_week_other_view = df[df.qsource_flag].drop_duplicates(['pax_id', 'week_num'])
pax_week_other_agg = pax_week_other_view.groupby(
    ['pax_id']
    )[['qsource_flag']].count().rename(
        columns={
            'qsource_flag':'qsource_week_count'
        }
    )
# Count total posts per week (including backops)
pax_week_other_view2  = df.groupby(
    ['week_num', 'pax_id'])[['bd_flag', 'blackops_flag']].sum()
pax_week_other_view2['bd_blackops_week'] = pax_week_other_view2['bd_flag'] + pax_week_other_view2['blackops_flag']
pax_week_other_agg2 = pax_week_other_view2.groupby(
    ['pax_id'])[['bd_blackops_week']].max().rename(
        columns={
            'bd_blackops_week': 'bd_blackops_week_max'
        }
    )


######################################
#           Monthly views            #
######################################
# Beatdowns only , month / ao level
pax_month_ao_view = df[df.bd_flag].groupby(
    ['month_num', 'ao_id', 'pax_id']
    )[['bd_flag', 'q_flag']].sum().rename(
    columns={
        'bd_flag':'bd', 'q_flag':'q'
    }
)
# Month level
pax_month_view = pax_month_ao_view.groupby(
    ['month_num', 'pax_id']
    )[['bd', 'q']].agg(
        ['sum', np.count_nonzero])
pax_month_view.columns = pax_month_view.columns.map('_'.join).str.strip('_')
pax_month_view.rename(
    columns={
        'bd_sum':'bd_sum_month', 'q_sum':'q_sum_month',
        'bd_count_nonzero':'bd_ao_count_month', 'q_count_nonzero':'q_ao_count_month'
    },
    inplace=True
)
# Monthly (not just beatdowns, includes QSources and Blackops)
pax_month_view_other = df.groupby(
    ['month_num', 'pax_id']
    )[['qsource_flag', 'blackops_flag']].sum().rename(
    columns={
        'qsource_flag':'qsource_sum_month', 'blackops_flag':'blackops_sum_month'
    }
)
# Aggregate to PAX level
pax_month_agg = pax_month_view.groupby(
    ['pax_id']
    ).max().rename(
        columns={
            'bd_sum_month':'bd_sum_month_max', 'q_sum_month':'q_sum_month_max',
            'bd_ao_count_month':'bd_ao_count_month_max', 'q_ao_count_month':'q_ao_count_month_max'
        }
)
pax_month_other_agg = pax_month_view_other.groupby(
    ['pax_id']
    ).max().rename(
        columns={
            'qsource_sum_month':'qsource_sum_month_max',
            'blackops_sum_month':'blackops_sum_month_max'
        }
)
# Number of unique AOs Q count
pax_month_q_view = df[df.q_flag].drop_duplicates(['month_num', 'pax_id', 'ao_id'])
pax_month_q_view2 = pax_month_q_view.groupby(
    ['month_num', 'pax_id']
    )[['q_flag']].count().rename(
    columns={
        'q_flag':'q_ao_count'
    }
)
pax_month_q_agg = pax_month_q_view2.groupby(
    ['pax_id']
    ).max().rename(
    columns={
        'q_ao_count':'q_ao_month_max'
    }
)

#####################################
#           Annual views            #
#####################################
# Beatdowns only, ao / annual level
pax_year_ao_view = df[df.bd_flag].groupby(
    ['ao_id', 'pax_id']
    )[['bd_flag', 'q_flag', 'forge_flag']].sum().rename(
    columns={
        'bd_flag':'bd', 'q_flag':'q', 'forge_flag':'forge'
    }
)
pax_year_view = pax_year_ao_view.groupby(
    ['pax_id']
    )[['bd', 'q', 'forge']].agg(
    ['sum', np.count_nonzero])
pax_year_view.columns = pax_year_view.columns.map('_'.join).str.strip('_')
pax_year_view.rename(
    columns={
        'bd_sum':'bd_sum_year', 'q_sum':'q_sum_year', 'forge_sum':'forge_sum_year',
        'bd_count_nonzero':'bd_ao_count_year', 'q_count_nonzero':'q_ao_count_year', 'forge_count_nonzero':'forge_count'
    },
    inplace=True
)
# Other than beatdowns
pax_year_view_other = df.groupby(
    ['pax_id']
    )[['qsource_flag', 'blackops_flag']].sum().rename(
    columns={
        'qsource_flag':'qsource_sum_year', 'blackops_flag':'blackops_sum_year'
    }
)
pax_year_ao_view = df[df.bd_flag].groupby(
    ['pax_id', 'ao_id']
    )[['bd_flag']].count().rename(
    columns={
        'bd_flag':'bd_sum_ao'
    }
)
pax_year_ao_agg = pax_year_ao_view.groupby(
    ['pax_id']
    )[['bd_sum_ao']].max().rename(
    columns={
        'bd_sum_ao':'bd_sum_ao_max'
    }
)

# Merge everything to PAX / annual view
# TODO: add weekly view for point summaries
pax_name_df = df[['pax_id', 'pax']].drop_duplicates(['pax_id', 'pax'])
merge_list = [
    pax_name_df,
    pax_year_view_other,
    pax_year_view,
    pax_year_ao_agg,
    pax_month_other_agg,
    pax_month_q_agg,
    pax_month_agg,
    pax_week_agg,
    pax_week_other_agg,
    pax_week_agg2,
    pax_week_other_agg2
]
pax_view = reduce(lambda left,right: pd.merge(left, right, on=['pax_id'], how='outer'), merge_list).fillna(0)

# Calculate automatic achievements
pax_view['the_priest'] = pax_view['qsource_sum_year'] >= 25
pax_view['the_monk'] = pax_view['qsource_sum_month_max'] >= 4
pax_view['leader_of_men'] = pax_view['q_sum_month_max'] >= 4
pax_view['the_boss'] = pax_view['q_sum_month_max'] >= 6
pax_view['be_the_hammer_not_the_nail'] = pax_view['q_sum_week_max'] >= 6
pax_view['cadre'] = pax_view['q_ao_month_max'] >= 7
pax_view['el_presidente'] = pax_view['q_sum_year'] >= 20
pax_view['6_pack'] = pax_view['bd_blackops_week_max'] >= 6
pax_view['el_quatro'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 25
pax_view['golden_boy'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 50
pax_view['centurion'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 100
pax_view['karate_kid'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 150
pax_view['crazy_person'] = pax_view['bd_sum_year'] + pax_view['blackops_sum_year'] >= 200
pax_view['holding_down_the_fort'] = pax_view['bd_sum_ao_max'] >= 50
pax_view['fire_and_steel'] = pax_view['forge_sum_year'] >= 20

# Flag manual acheivements from tagged backblasts
man_achievement_df = df.loc[~(df.achievement.isna()), ['pax_id', 'achievement']].drop_duplicates(['pax_id', 'achievement'])
man_achievement_df['achieved'] = True
man_achievement_df = man_achievement_df.pivot(index=['pax_id'], columns=['achievement'], values=['achieved'])

# Merge to PAX view
man_achievement_df = man_achievement_df.droplevel(0, axis=1).reset_index()
pax_view = pd.merge(pax_view, man_achievement_df, on=['pax_id'], how='left')

# Load achievement / awarded tables and merge
achievement_list = pd.read_csv('data/achievement_list.csv')
awarded_table = pd.read_csv('data/awarded_table.csv').set_index('pax_id')
pax_view = pd.merge(pax_view, awarded_table, how='left', on='pax_id', suffixes=("", "_awarded"))

# instantiate Slack client
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
slack_client = WebClient(slack_secret, ssl=ssl_context)

# Loop through achievement list, looking for achievements earned but not yet awarded
award_count = 0
for index, row in achievement_list.iterrows():
    award = row['code']
    
    # check to see if award has been earned anywhere and / or has been awarded
    if award + '_awarded' in pax_view.columns:
        new_awards = pax_view[(pax_view[award] == True) & (pax_view[award + '_awarded'] != True)]
    elif award in pax_view.columns:
        new_awards = pax_view[pax_view[award] == True]
    else:
        new_awards = pd.DataFrame()

    if len(new_awards) > 0:
        for pax_index, pax_row in new_awards.iterrows():
            # mark off in the awarded table as awarded for that PAX
            awarded_table.loc[pax_row['pax_id'], award + '_awarded'] = True
            achievements_to_date = awarded_table.loc[pax_row['pax_id']].count()
            # send to slack channel
            sMessage = f"Congrats to our man <@{pax_row['pax_id']}>! He just unlocked the achievement *{row['name']}* for {row['verb']}. This is achievement #{achievements_to_date} for <@{pax_row['pax_id']}> this year. Keep up the good work!"
            response = slack_client.chat_postMessage(channel=achievement_channel, text=sMessage, link_names=True)
            response2 = slack_client.reactions_add(channel=achievement_channel, name='fire', timestamp=response['ts'])
            award_count += 1
            
            

# Save awarded table for future reference
awarded_table.to_csv('data/awarded_table.csv')

# Send confirmation message to myself
response = slack_client.chat_postMessage(channel='U025H3PM1S9', text=f'Patch program run for the day, {award_count} awards tagged')
print("All done!")

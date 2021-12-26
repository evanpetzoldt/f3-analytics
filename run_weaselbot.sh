#!/bin/bash -l

cd /mnt/nas/ml/f3-analytics
source /mnt/nas/ml/f3-analytics/env/bin/activate
/mnt/nas/ml/f3-analytics/env/bin/python /mnt/nas/ml/f3-analytics/build_master_table.py
/mnt/nas/ml/f3-analytics/env/bin/python /mnt/nas/ml/f3-analytics/weaselbot.py
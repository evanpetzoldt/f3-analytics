# f3-analytics
Scripts to analyze F3 posting data, as captured by PAXMiner. Also includes code to control a Slack bot named Weaselbot, which currently can send reminders to site Qs about PAX who haven't posted or Q'd in a while. I have a lot more planned!

## Installation

First, I am using `Python 3.8.10 (64-bit)` for this project... not sure if other versions would work or not. Currently runs off of an `Ubuntu 20.04` server.

Second, I used `venv` to manage my packages... here are the approximate steps you'd need to replicate:
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Lastly, for security purposes I have the slack bot API secret key and PAXMiner database password stored in a `.env` file which is imported in the code, but is not tracked by git. Reach out to me privately for these keys.

For running Weaselbot, you will need to create your own `data/siteq.csv` list... the format for that is the following:
| ao         | site_q | user_id |
|:----------:|:------:|:-------:|
| "ao-eagles-nest" | "Moneyball" | "U025H3PM1S9" |
| ...      |  ... |          ... |

## Running Weaselbot

Information on Weaselbot is located here: https://api.slack.com/apps/A02K3PSN7C6. You may not have access, reach out to me if you would like it (I think you'll need to be on STC's Slack).

I activate Weaselbot once a week on Sunday mornings using `run_weaselbot.sh`. You would need to update the file locations in that script.

I utilize `Cron` to run this script. To run from `Cron`, run `crontab -e`, and then add something similar to:
```
SHELL=/bin/bash
7 14 * * 0 /mnt/nas/ml/f3-analytics/run_weaselbot.sh > /tmp/f3cron.log 2>&1
```
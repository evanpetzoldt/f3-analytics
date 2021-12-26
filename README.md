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
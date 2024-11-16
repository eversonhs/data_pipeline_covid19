import subprocess
from datetime import date, timedelta

today = date.today()
last_date = date(2024, 9, 11)
dt= last_date

while dt != today:
    dt_str = dt.strftime("%Y-%m-%d")
    print(dt_str)
    sub = subprocess.run(rf'python gcloud\jobs\incremental\bronze_vacinacao_covid_19_json.py --date={dt_str}')   
    dt = dt + timedelta(days=1)

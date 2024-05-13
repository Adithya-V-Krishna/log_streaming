import requests
import json
import random
import decimal
import time
from datetime import datetime

def make_post_request(url, data):

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json.dumps(data), headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

reel_id = [10345, 10678, 10500, 10123, 10987]
reel_category = ["Comedy", "Dance",  "DIY",  "Travel", "Fashion"]
created_account_id = [11456, 11789, 11234, 11678, 11900]
reel_length = [25.72, 33.94, 41.23, 28.56, 46.81]
reel_size = [7.83, 6.21, 4.56, 8.97, 5.34]

ad_id = [57023, 51842, 53697, 55311, 56349, 59275, 52764, 59621, 50237, 51186]
ad_category = ["Automotive", "Technology", "Fashion", "Food", "Tourism", 
               "Health", "Garden", "Entertainment", "Financial_Services", "Education"]

carrier = ["jio", "vi", "airtel", "BSNL", "wi-fi" ]

ad_skipped_and_shown = ["true", "false"]



url = 'http://127.0.0.1:8000/data/push/'

for i in range(0,10):
    reel_choice = random.choice(reel_id)
    index = reel_id.index(reel_choice)  

    ad_choice = random.choice(ad_id)
    ad_index = ad_id.index(ad_choice)  

    ad_shown = random.choice(ad_skipped_and_shown)
    if ad_shown == "false":
        ad_skipped = "false"
    else :
        ad_skipped = random.choice(ad_skipped_and_shown)

    current_time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    current_time = datetime.strptime(current_time_str, "%Y-%m-%d %H:%M:%S")
    truncated_time = current_time.replace(minute=0, second=0)
    truncated_time_str = truncated_time.strftime("%Y-%m-%d %H:%M:%S")      
    
    data = {
        "reel_id" : str(reel_choice),
        "reel_category" : str(reel_category[index]),
        "created_account_id" : str(created_account_id[index]),
        "user_account_id" : str(random.randint(21000, 21999)),
        "reel_length" : str(reel_length[index]),
        "reel_size" : str(reel_size[index]),
        "touch_point" : str(random.randint(0, 5)),
        "reel_spent_time" : '{}'.format(decimal.Decimal(random.randrange(0, 100*(reel_length[index])))/100), 
        "ad_id" : str(ad_choice),
        "ad_spent_time" : '{}'.format(decimal.Decimal(random.randrange(0, 100*(reel_length[index])))/100),
        "ad_shown" : ad_shown,
        "ad_skipped" : ad_skipped,
        "ad_category" : str(ad_category[ad_index]),
        "carrier" : random.choice(carrier),
        "latitude" : '{}'.format(decimal.Decimal(random.randrange(80, 130))/10),
        "longitude" :  '{}'.format(decimal.Decimal(random.randrange(740, 780))/10),
        "created_at" : truncated_time_str
    } 

    response = make_post_request(url, data)
    

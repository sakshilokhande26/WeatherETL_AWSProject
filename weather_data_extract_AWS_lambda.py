import json
import os
import requests
from datetime import date, timedelta, datetime
import boto3

def lambda_handler(event, context):
    xrapidapikey = os.environ.get('xrapidapikey')
    xrapidapihost = os.environ.get('xrapidapihost')
    
    url = "https://weatherapi-com.p.rapidapi.com/history.json"
    
    dt = date.today()-timedelta(days=5)
    end_dt = date.today()-timedelta(days=0)
    
    querystring = {"q":"Mumbai", "lang":"en","dt":dt,"end_dt":end_dt}
    
    headers = {
	"x-rapidapi-key": xrapidapikey, "x-rapidapi-host": xrapidapihost
              }
              
    response = requests.get(url, headers=headers, params=querystring)
    m_weather_data = response.json()
    
    client =boto3.client('s3')
    
    filename = "weather_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = "weather-etl-project-sakshi",
        Key = "raw_data/to-process/" + filename ,
        Body = json.dumps(m_weather_data)
        
        )


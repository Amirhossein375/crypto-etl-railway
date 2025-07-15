import requests 
import pandas as pd
from sqlalchemy import create_engine

db_url = 'postgresql://postgres:KYtkFoBtUIdmczCHfsnPixIQOsrMANSR@yamabiko.proxy.rlwy.net:32937/railway'

engine = create_engine(db_url)

def fetch_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency" : "usd", "order" : "market_cap_desc"}
    r = requests.get(url,params = params)
    return r.json()
def clean_data(raw):
    df=pd.DataFrame(raw)
    df= df[['id', 'symbol', 'current_price', 'market_cap', 'total_volume']]
    df['fetched_at'] = pd.Timestamp.now()
    return df
def save_to_db(df):
    df.to_sql("crypto_prices", engine, if_exsit = 'append', index = False)
    print("✅ داده‌ها ذخیره شدند")
    
if __name__ == '__main__':
    raw = fetch_data()
    df = clean_data(raw)
    save_to_db(df)
 

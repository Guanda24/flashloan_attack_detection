from pycoingecko import CoinGeckoAPI
import pandas as pd
import time
import datetime

def get_token_price_history(token_address, date):
    cg = CoinGeckoAPI()
    date = int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())
    
    price_data = cg.get_coin_market_chart_range_by_id(
        id = 'ethereum',
        vs_currency = 'usd',
        from_timestamp = date,
        to_timestamp = date + 86400,
        localization = False,
        contract_address = token_address
    )
    
    df = pd.DataFrame(price_data['prices'],columns=['timestamp','price'])
    df['date']=pd.to_datetime(df['timestamp'],unit='ms').dt.strftime('%Y-%m-%d')
    return df

def get_ETH_price_history(date):
    cg = CoinGeckoAPI()
    date = int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())
    
    price_data = cg.get_coin_market_chart_range_by_id(
        id = 'ethereum',
        vs_currency = 'usd',
        from_timestamp = date,
        to_timestamp = date + 86400
    )
    
    df = pd.DataFrame(price_data['prices'],columns=['timestamp','price'])
    df['date']=pd.to_datetime(df['timestamp'],unit='ms').dt.strftime('%Y-%m-%d')
    return df
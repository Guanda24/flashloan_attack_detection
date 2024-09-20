from pycoingecko import CoinGeckoAPI
from tqdm import tqdm
from multiprocessing import Pool
import pandas as pd
import datetime

flashloan_unique_token = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_unique_token.csv')['address'].tolist()

# Initialize CoinGeckoAPI
cg = CoinGeckoAPI()

# start_timestamp and end_timestamp
Balancer_v2_created_timestamp = 12272146
start_timestamp = int(pd.to_datetime(12272146, unit='s').timestamp())
end_timestamp = int(datetime.datetime(2023, 12, 31).timestamp())

# Initialise token_price dataframe
token_price = pd.DataFrame(columns=['token_address', 'price', 'date', 'timestamp'])

def fetch_price_data(token_address):
    try:
        price_data = cg.get_coin_market_chart_range_by_id(
            id='ethereum',
            vs_currency='usd',
            from_timestamp=start_timestamp,
            to_timestamp=end_timestamp,
            localization=False,
            contract_address=token_address
        )
        df = pd.DataFrame(price_data['prices'], columns=['timestamp', 'price'])
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d')
        df['token_address'] = token_address
        return df
    except Exception as e:
        print(f"{e} at {token_address}")
        return None   
    
with Pool(processes=2) as pool:  # Adjust the number of processes as needed
    for df in tqdm(pool.imap_unordered(fetch_price_data, flashloan_unique_token), total=len(flashloan_unique_token)):
        if df is not None:
            token_price = pd.concat([token_price, df], axis=0, ignore_index=True)

token_price.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_token_price.csv', index=False)
from pycoingecko import CoinGeckoAPI
from tqdm import tqdm
from multiprocessing import Pool
import pandas as pd
import datetime

Uniswap_v3_flashloan_unique_token = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_unique_token.csv')['address'].tolist()

# Initialize CoinGeckoAPI
cg = CoinGeckoAPI()

# start_timestamp and end_timestamp
Uniswap_v3_created_timestamp = 1620156420 # Uniswap v3 contract factory created timestamp
start_timestamp = int(pd.to_datetime(1620156420, unit='s').timestamp())
end_timestamp = int(datetime.datetime(2023, 12, 31).timestamp())

# Initialise Uniswap_v3_token_price dataframe
Uniswap_v3_token_price = pd.DataFrame(columns=['token_address', 'price', 'date', 'timestamp'])

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
    for df in tqdm(pool.imap_unordered(fetch_price_data, Uniswap_v3_flashloan_unique_token), total=len(Uniswap_v3_flashloan_unique_token)):
        if df is not None:
            Uniswap_v3_token_price = pd.concat([Uniswap_v3_token_price, df], axis=0, ignore_index=True)

Uniswap_v3_token_price.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_token_price.csv', index=False)
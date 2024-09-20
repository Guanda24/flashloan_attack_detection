from web3 import Web3
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
import ABC_without_symbol as AB
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#

flash_tx_unique = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_flashloan_unique.csv')
tx_hash_list = flash_tx_unique['tx_hash'].tolist()

def process_tx_hash(tx_hash):
    ABC = AB.get_account_balance_change_with_default_abi(tx_hash)
    token_address_temp = [col for col in ABC.columns if col != 'address']
    return token_address_temp

num_processes = 64

with Pool(num_processes) as pool:
    # Use tqdm to track progress
    results = list(tqdm(pool.imap_unordered(process_tx_hash, tx_hash_list), total=len(tx_hash_list)))

token_address_list = list(set(item for sublist in results for item in sublist))

token_address_df = pd.DataFrame(token_address_list, columns = ['address'])

token_address_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_unique_token.csv', index=False)
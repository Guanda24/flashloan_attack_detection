from web3 import Web3
from datetime import datetime, timedelta
import vaex
import pandas as pd
import matplotlib.pyplot as plt
import Account_Balance as AB
from tqdm import tqdm
from multiprocessing import Pool
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 200}))#

swap_events = vaex.open('/srv/abacus-3/MP_Defi_txs_TY_23/new_uni-v2-swaps_sender_to_equal.hdf5')

def v2_swap_or_flashloan(tx, sc_address):
    uniswapV2Call_func = '0x10d1e85c'
    tx_trace = w3.tracing.trace_transaction(tx)

    for i in range (len(tx_trace)):
        if 'input' in tx_trace[i]['action'] and 'error' not in tx_trace[i]:
            if uniswapV2Call_func == tx_trace[i]['action']['input'].hex()[:10] and tx_trace[i]['action']['from'] == sc_address:
                # flashloan
                return 1
    # swap
    return 0
                    
                    
def process_swap_event(i):
    result = v2_swap_or_flashloan(swap_events[i][2], AB.convert_to_checksum_address(swap_events[i][7]))
    if result == 1:
        return swap_events[i]
    else:
        return None

def get_v2_daily_flash_tx():
    v2_daily_flash_tx_logs = []
    
    columns_name = ['block_number', 'timestamp', 'tx_hash', 'transactionIndex', 'log_index', 'sender', 'to', 'pair_contract_address', 'amount0_in', 'amount1_in', 'amount0_out', 'amount1_out', 'date']
    
    num_processes = 64  # Adjust this based on your system and workload
    
    with Pool(num_processes) as pool:
        for result in tqdm(pool.imap(process_swap_event, range(len(swap_events))), total=len(swap_events)):
            if result is not None:
                v2_daily_flash_tx_logs.append(result)

    v2_daily_flash_tx_logs_df = pd.DataFrame(v2_daily_flash_tx_logs, columns=columns_name).sort_values(by='timestamp').reset_index(drop=True)
    v2_daily_flash_tx_logs_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/v2_daily_flash_tx_logs.csv', index=False)
    
    min_indices = v2_daily_flash_tx_logs_df.groupby('tx_hash')['log_index'].idxmin()
    v2_daily_flash_tx_unique_df = v2_daily_flash_tx_logs_df.loc[min_indices].sort_values(by='timestamp').reset_index(drop=True)
    v2_daily_flash_tx_unique_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/v2_daily_flash_tx_unique.csv', index=False)

    v2_daily_flash_tx_count_df = v2_daily_flash_tx_unique_df.groupby('date').size().reset_index(name='daily_flash_tx_count')
    v2_daily_flash_tx_count_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/v2_daily_flash_tx_count.csv', index=False)

    return v2_daily_flash_tx_logs_df, v2_daily_flash_tx_unique_df, v2_daily_flash_tx_count_df

v2_daily_flash_tx_logs_df, v2_daily_flash_tx_unique_df, v2_daily_flash_tx_count_df= get_v2_daily_flash_tx()


# Plotting
plt.figure(figsize=(20, 12))
plt.bar(v2_daily_flash_tx_count_df['date'], v2_daily_flash_tx_count_df['daily_flash_tx_count'], color='blue')
plt.title('v2 Daily Flash Transactions')
plt.xlabel('Date')
plt.ylabel('v2 Flashloan Transaction Count')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.tight_layout()
plt.savefig('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/v2_daily_flash_transactions.png')
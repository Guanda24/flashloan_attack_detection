from web3 import Web3
from datetime import datetime, timedelta
import vaex
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 200}))#

swap_events = vaex.open('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/uni-v2-sender_equal_to_21089068.hdf5') # Until 2024-10-31

columns_name = ['block_number', 'timestamp', 'tx_hash', 'log_index', 'tx_index', 'pair_contract_address', 'sender', 'to', 'amount0In', 'amount1In', 'amount0Out', 'amount1Out']

timestamp_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/timestamp_21145534.csv')
timestamp_df['real_life_time'] = pd.to_datetime(timestamp_df['timestamp'], unit='s')
timestamp_df['date'] = timestamp_df['real_life_time'].dt.date
timestamp_df = timestamp_df[10000835:]

uniswap_v2_abi = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

def convert_to_checksum_address(hex_address):

    normalized_address = '0x' + hex_address[-40:]
    checksummed_address = w3.to_checksum_address(normalized_address)
    
    return checksummed_address

def Uniswap_v2_swap_or_flashloan(tx, sc_address):
    uniswapV2Call_func = '0x10d1e85c'
    tx_trace = w3.tracing.trace_transaction(tx)

    for i in range (len(tx_trace)):
        if 'input' in tx_trace[i]['action'] and 'error' not in tx_trace[i]:
            if uniswapV2Call_func == tx_trace[i]['action']['input'].hex()[:10] and tx_trace[i]['action']['from'] == sc_address:
                # flashloan
                return 1
    # swap
    return 0       

def process_flash_logs(i):
    event = swap_events[i]
    tx_hash = event[2]
    sc_address = convert_to_checksum_address(event[5])

    if Uniswap_v2_swap_or_flashloan(tx_hash, sc_address) == 1:  # if flashloan
        amount = event[10] if event[10] != 0 else event[11]
        smart_contract = w3.eth.contract(address=sc_address, abi=uniswap_v2_abi)
        flashloan_token = (
            smart_contract.functions.token0().call()
            if event[10] != 0
            else smart_contract.functions.token1().call()
        )
        # save results
        return {
            'block_number': event[0],
            'timestamp': event[1],
            'tx_hash': event[2],
            'transactionIndex': event[4],
            'logIndex': event[3],
            'sender': event[6],
            'to': event[7],
            'pair_contract_address': event[5],
            'amount0_in': event[8],
            'amount1_in': event[9],
            'amount0_out': event[10],
            'amount1_out': event[11],
            'date': timestamp_df['date'][timestamp_df['block_number'] == event[0]].values[0],
            'token': flashloan_token,
            'amount': amount,
        }
    return None  # if not flashloan，return None

num_processes = 64 
results = []
with Pool(num_processes) as pool:
    for result in tqdm(pool.imap(process_flash_logs, range(len(swap_events))), total=len(swap_events)):
        if result is not None:
            results.append(result)

Uniswap_v2_flashloan_logs_df = pd.DataFrame(results).sort_values(by='timestamp').reset_index(drop=True)

min_indices = Uniswap_v2_flashloan_logs_df.groupby('tx_hash')['logIndex'].idxmin()
Uniswap_v2_flashloan_unique_df = Uniswap_v2_flashloan_logs_df.loc[min_indices].sort_values(by='timestamp').reset_index(drop=True)
Uniswap_v2_flashloan_count_df = Uniswap_v2_flashloan_unique_df.groupby('date').size().reset_index(name='daily_flash_tx_count')


Uniswap_v2_flashloan_logs_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_logs.csv', index=False)
Uniswap_v2_flashloan_unique_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_unique.csv', index=False)
Uniswap_v2_flashloan_count_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_count.csv', index=False)
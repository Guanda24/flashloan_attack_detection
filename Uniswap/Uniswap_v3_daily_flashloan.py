from web3 import Web3
from datetime import datetime, timedelta
import Account_Balance as AB
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from multiprocessing import Pool, Manager
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#

Uniswap_v3_Pools_TY_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/poolsv3.csv')
timestamp_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/timestamp_21145534.csv')

timestamp_df['real_life_time'] = pd.to_datetime(timestamp_df['timestamp'], unit='s')
timestamp_df['date'] = timestamp_df['real_life_time'].dt.date
Uniswap_v3_timestamp_df = timestamp_df[Uniswap_v3_Pools_TY_df['createdAtBlockNumber'][0]:]

def process_flash_logs(i):
    start_block = int(Uniswap_v3_Pools_TY_df['createdAtBlockNumber'][i])
    end_block = 21089068
    
    contract_address = AB.convert_to_checksum_address(Uniswap_v3_Pools_TY_df['id'][i])
    abi = '[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]'
    contract = w3.eth.contract(address=contract_address, abi=abi)           
    current_pair_flash_logs = contract.events.Flash().get_logs(fromBlock=start_block,toBlock=end_block)
    if current_pair_flash_logs:
        for j in range(len(current_pair_flash_logs)):
            if current_pair_flash_logs[j]['args']['amount0'] == 0:
                info = {
                    'sender': current_pair_flash_logs[j]['args']['sender'],
                    'recipient': current_pair_flash_logs[j]['args']['recipient'],
                    'token0.symbol': Uniswap_v3_Pools_TY_df['token0.symbol'][i],
                    'token1.symbol': Uniswap_v3_Pools_TY_df['token1.symbol'][i],     
                    'token0.decimals': Uniswap_v3_Pools_TY_df['token0.decimals'][i],
                    'token1.decimals': Uniswap_v3_Pools_TY_df['token1.decimals'][i],
                    'amount0': current_pair_flash_logs[j]['args']['amount0'],
                    'amount1': current_pair_flash_logs[j]['args']['amount1'],
                    'paid0': current_pair_flash_logs[j]['args']['paid0'],
                    'paid1': current_pair_flash_logs[j]['args']['paid1'],
                    'pair_contract_address': Uniswap_v3_Pools_TY_df['id'][i],
                    'tx_hash': current_pair_flash_logs[j]['transactionHash'].hex(),
                    'logIndex': current_pair_flash_logs[j]['logIndex'], 
                    'transactionIndex': current_pair_flash_logs[j]['transactionIndex'],
                    'block_number': current_pair_flash_logs[j]['blockNumber'],
                    'timestamp': Uniswap_v3_timestamp_df['timestamp'][Uniswap_v3_timestamp_df['block_number'] == current_pair_flash_logs[j]['blockNumber']].values[0],
                    'date': Uniswap_v3_timestamp_df['date'][Uniswap_v3_timestamp_df['block_number'] == current_pair_flash_logs[j]['blockNumber']].values[0],
                    'token': Uniswap_v3_Pools_TY_df['token1.id'][i],
                    'amount': current_pair_flash_logs[j]['args']['amount1']
                }
            else:
                info = {
                    'sender': current_pair_flash_logs[j]['args']['sender'],
                    'recipient': current_pair_flash_logs[j]['args']['recipient'],
                    'token0.symbol': Uniswap_v3_Pools_TY_df['token0.symbol'][i],
                    'token1.symbol': Uniswap_v3_Pools_TY_df['token1.symbol'][i],     
                    'token0.decimals': Uniswap_v3_Pools_TY_df['token0.decimals'][i],
                    'token1.decimals': Uniswap_v3_Pools_TY_df['token1.decimals'][i],
                    'amount0': current_pair_flash_logs[j]['args']['amount0'],
                    'amount1': current_pair_flash_logs[j]['args']['amount1'],
                    'paid0': current_pair_flash_logs[j]['args']['paid0'],
                    'paid1': current_pair_flash_logs[j]['args']['paid1'],
                    'pair_contract_address': Uniswap_v3_Pools_TY_df['id'][i],
                    'tx_hash': current_pair_flash_logs[j]['transactionHash'].hex(),
                    'logIndex': current_pair_flash_logs[j]['logIndex'], 
                    'transactionIndex': current_pair_flash_logs[j]['transactionIndex'],
                    'block_number': current_pair_flash_logs[j]['blockNumber'],
                    'timestamp': Uniswap_v3_timestamp_df['timestamp'][Uniswap_v3_timestamp_df['block_number'] == current_pair_flash_logs[j]['blockNumber']].values[0],
                    'date': Uniswap_v3_timestamp_df['date'][Uniswap_v3_timestamp_df['block_number'] == current_pair_flash_logs[j]['blockNumber']].values[0],
                    'token': Uniswap_v3_Pools_TY_df['token0.id'][i],
                    'amount': current_pair_flash_logs[j]['args']['amount0']
                }
            Uniswap_v3_flashloan_logs.append(info)
            

# Manager to share variables between processes
manager = Manager()
Uniswap_v3_flashloan_logs = manager.list()

num_processes = 64  # Adjust this based on your system and workload   
with Pool(num_processes) as pool:
    list(tqdm(pool.imap(process_flash_logs, range(len(Uniswap_v3_Pools_TY_df))), total=len(Uniswap_v3_Pools_TY_df)))
    
print('initialise')
Uniswap_v3_flashloan_logs = list(Uniswap_v3_flashloan_logs)

print('to df')
Uniswap_v3_flashloan_logs_df = pd.DataFrame(Uniswap_v3_flashloan_logs).sort_values(by='timestamp').reset_index(drop=True)
min_indices = Uniswap_v3_flashloan_logs_df.groupby('tx_hash')['logIndex'].idxmin()
Uniswap_v3_flashloan_unique_df = Uniswap_v3_flashloan_logs_df.loc[min_indices].sort_values(by='timestamp').reset_index(drop=True)
Uniswap_v3_flashloan_count_df = Uniswap_v3_flashloan_unique_df.groupby('date').size().reset_index(name='daily_flash_tx_count')

print('to_csv')
Uniswap_v3_flashloan_logs_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_logs.csv', index=False)
Uniswap_v3_flashloan_unique_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_unique.csv', index=False)
Uniswap_v3_flashloan_count_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_count.csv', index=False)
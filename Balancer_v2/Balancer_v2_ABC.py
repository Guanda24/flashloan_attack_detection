from web3 import Web3
from web3.logs import DISCARD
import pandas as pd
import vaex
from tqdm import tqdm
from multiprocessing import Pool, Manager
from decimal import Decimal, getcontext
import ABC_without_symbol as AB

w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#
getcontext().prec = 50

# Manager to share variables between processes
manager = Manager()
Balancer_v2_ABC = manager.list()
Balancer_v2_ABC_error_index = manager.list()
Balancer_v2_price_cannot_get = manager.list()

Balancer_v2_flashloan_unique = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_flashloan_unique.csv')
token_price = pd.read_csv('/home/user/gzhao/Thesis/Price/token_price_filtered.csv')
Uniswap_v2_sync = vaex.open('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v2-sync_drop_duplicates.hdf5')
Uniswap_v3_swap = vaex.open('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v3-swap-drop-duplicates.hdf5')

try:
    Balancer_v2_ABC_error_tx_cant_be_solved = manager.list(pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC_error_tx_cant_be_solved.csv', header=None)[0].tolist())
except Exception as e:
    Balancer_v2_ABC_error_tx_cant_be_solved = manager.list()
    
Uniswap_v2_pair_contract_abi = '[{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1In","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount0Out","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1Out","type":"uint256"},{"indexed":true,"internalType":"address","name":"to","type":"address"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"MINIMUM_LIQUIDITY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"_reserve0","type":"uint112"},{"internalType":"uint112","name":"_reserve1","type":"uint112"},{"internalType":"uint32","name":"_blockTimestampLast","type":"uint32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_token0","type":"address"},{"internalType":"address","name":"_token1","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"kLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"mint","outputs":[{"internalType":"uint256","name":"liquidity","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"price0CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"price1CumulativeLast","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"}],"name":"skim","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount0Out","type":"uint256"},{"internalType":"uint256","name":"amount1Out","type":"uint256"},{"internalType":"address","name":"to","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"sync","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]'

Uniswap_v3_pair_contract_abi = '[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]'

def get_price_change_ratio(tx_hash):
    tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
    
    reserve_before_swap_list = []
    reserve_after_swap_list = []
    price_change_ratio_list =[]
    
    # Uniswap V2 swap events
    Uniswap_v2_pair_contract = w3.eth.contract(abi=Uniswap_v2_pair_contract_abi) 
    Uniswap_v2_swap_events = Uniswap_v2_pair_contract.events.Swap().process_receipt(tx_receipt,DISCARD)
    Uniswap_v2_swap_events = [dict(i) for i in Uniswap_v2_swap_events]
    Uniswap_v2_num_swap_events = len(Uniswap_v2_swap_events)
    Uniswap_v2_logIndex_list = [event['logIndex'] for event in Uniswap_v2_swap_events]
    Uniswap_v2_address_list = [event['address'].lower() for event in Uniswap_v2_swap_events]
 
    for i in range(Uniswap_v2_num_swap_events):
        reserve0_before_swap = Decimal('NaN')
        reserve1_before_swap = Decimal('NaN')
        price_before_swap = Decimal('NaN') 
        reserve0_after_swap = Decimal('NaN')
        reserve1_after_swap = Decimal('NaN')
        price_after_swap = Decimal('NaN')
        price_change_ratio = Decimal('NaN')
        sync_df = Uniswap_v2_sync[Uniswap_v2_sync['pair_contract_address'] == Uniswap_v2_address_list[i]]
        if sync_df:
            sync_df = sync_df.sort(by=['block_number', 'tx_index', 'log_index'], ascending=[True, True, True]).to_pandas_df()
            if not sync_df[(sync_df['tx_hash'] == tx_hash ) & (sync_df['log_index'] == Uniswap_v2_logIndex_list[i] - 1)].empty:
                # reserve after swap
                index = sync_df[(sync_df['tx_hash'] == tx_hash ) & (sync_df['log_index'] == Uniswap_v2_logIndex_list[i] - 1)].index[0]
                reserve0_after_swap = Decimal(sync_df['reserve0'][index])
                reserve1_after_swap = Decimal(sync_df['reserve1'][index])
                price_after_swap = reserve0_after_swap / reserve1_after_swap
                # reserve before swap
                if index == 0:
                    reserve0_before_swap = Decimal('NaN')
                    reserve1_before_swap = Decimal('NaN')
                    price_before_swap = Decimal('NaN')
                else:
                    index = index - 1
                    reserve0_before_swap = Decimal(sync_df['reserve0'][index])
                    reserve1_before_swap = Decimal(sync_df['reserve1'][index])
                    price_before_swap = reserve0_before_swap / reserve1_before_swap
                # reserve ratio
                price_change_ratio = (price_after_swap - price_before_swap) / price_before_swap
        # Append result to list    
        reserve_before_swap_list.append({'reserve0': reserve0_before_swap, 'reserve1': reserve1_before_swap, 'price': price_before_swap})
        reserve_after_swap_list.append({'reserve0': reserve0_after_swap, 'reserve1': reserve1_after_swap, 'price': price_after_swap})
        price_change_ratio_list.append(price_change_ratio)
    
    # UniswapV3 swap events
    Uniswap_v3_pair_contract = w3.eth.contract(abi=Uniswap_v3_pair_contract_abi) 
    Uniswap_v3_swap_events = Uniswap_v3_pair_contract.events.Swap().process_receipt(tx_receipt,DISCARD)
    Uniswap_v3_swap_events = [dict(i) for i in Uniswap_v3_swap_events]
    Uniswap_v3_num_swap_events = len(Uniswap_v3_swap_events)
    Uniswap_v3_logIndex_list = [event['logIndex'] for event in Uniswap_v3_swap_events]
    Uniswap_v3_address_list = [event['address'].lower() for event in Uniswap_v3_swap_events]

    for i in range(Uniswap_v3_num_swap_events):
        reserve0_before_swap = Decimal('NaN')
        reserve1_before_swap = Decimal('NaN')
        price_before_swap = Decimal('NaN') 
        reserve0_after_swap = Decimal('NaN')
        reserve1_after_swap = Decimal('NaN')
        price_after_swap = Decimal('NaN')
        price_change_ratio = Decimal('NaN')
        swap_df = Uniswap_v3_swap[Uniswap_v3_swap['pair_contract_address'] == Uniswap_v3_address_list[i]]
        if swap_df:
            swap_df = swap_df.sort(by=['block_number'], ascending=[True]).to_pandas_df()
            # reserve after swap
            liquidity = Decimal(Uniswap_v3_swap_events[i]['args']['liquidity'])
            sqrtPriceX96 = Decimal(Uniswap_v3_swap_events[i]['args']['sqrtPriceX96'])
            price_after_swap = Decimal((sqrtPriceX96 ** 2) / (2 ** 192))
            reserve0_after_swap = Decimal(round(liquidity / price_after_swap.sqrt()))
            reserve1_after_swap = Decimal(round(liquidity * price_after_swap.sqrt()))    
            # reserve before swap 
            if not swap_df[(swap_df['pair_contract_address'] == Uniswap_v3_address_list[i]) & (swap_df['tx_hash'] == tx_hash) & (swap_df['log_index'] == Uniswap_v3_logIndex_list[i])].empty:
                index = swap_df[(swap_df['pair_contract_address'] == Uniswap_v3_address_list[i]) & (swap_df['tx_hash'] == tx_hash) & (swap_df['log_index'] == Uniswap_v3_logIndex_list[i])].index[0]
                if index != 0 :
                    index = index - 1
                    liquidity = Decimal(swap_df['liquidity'][index])
                    sqrtPriceX96 = Decimal(swap_df['sqrt_price_x96'][index])
                    price_before_swap = Decimal((sqrtPriceX96 ** 2) / (2 ** 192))
                    reserve0_before_swap = Decimal(round(liquidity / price_before_swap.sqrt()))
                    reserve1_before_swap = Decimal(round(liquidity * price_before_swap.sqrt()))     
                else:
                    reserve0_before_swap = Decimal('NaN')
                    reserve1_before_swap = Decimal('NaN')
                    price_before_swap = Decimal('NaN')
            # price change ratio
            price_change_ratio = (price_after_swap - price_before_swap) / price_before_swap
        # Append result to list    
        reserve_before_swap_list.append({'reserve0': reserve0_before_swap, 'reserve1': reserve1_before_swap, 'price': price_before_swap})
        reserve_after_swap_list.append({'reserve0': reserve0_after_swap, 'reserve1': reserve1_after_swap, 'price': price_after_swap})
        price_change_ratio_list.append(price_change_ratio)
              
    filtered_price_change_ratio_list = [x for x in price_change_ratio_list if x.is_finite()]
    if not filtered_price_change_ratio_list:
        highest_price_change_ratio = Decimal('NaN')
    else:
        highest_price_change_ratio = max(abs(x) for x in filtered_price_change_ratio_list)
        
    num_swap_events = Uniswap_v2_num_swap_events + Uniswap_v3_num_swap_events
    return num_swap_events, reserve_before_swap_list, reserve_after_swap_list, price_change_ratio_list, highest_price_change_ratio        
            
def get_ABC_in_usd(tx_hash, from_address, to_address, ABC_simplified, date):
    ABC_in_usd = ABC_simplified
    token_address_list = [col for col in ABC_in_usd.columns if col != 'address']
    price_list = []
    
    # gas fee
    gas_price = Decimal(w3.eth.get_transaction(tx_hash).gasPrice)
    gas_used = Decimal(w3.eth.get_transaction_receipt(tx_hash).gasUsed)
    gas_fee = (gas_price * gas_used) / Decimal(10**18)
    ETH_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ETH_index = token_price[(token_price['token.id'] == ETH_address) & (token_price['date'] == date)].index[0]
    ETH_price = Decimal(token_price['priceUSD'][ETH_index])
    gas_fee_in_usd = gas_fee * ETH_price
    
    for i in range(len(token_address_list)):
        # iterate through each column to get price of all tokens
        token_address = token_address_list[i]
        if not token_price[(token_price['token.id'] == token_address) & (token_price['date'] == date)].empty:
            index = token_price[(token_price['token.id'] == token_address) & (token_price['date'] == date)].index[0]
            price = Decimal(token_price['priceUSD'][index])
        else:
            price = Decimal('NaN')
            if token_address not in Balancer_v2_price_cannot_get:
                Balancer_v2_price_cannot_get.append(token_address)

        price_list.append({'token_address': token_address, 'price': price})
        
        # iterate through each row to calculate the balance change in usd
        for j in range(ABC_in_usd.shape[0]):
            amount_in_usd = ABC_in_usd[token_address][j] * price
            ABC_in_usd[token_address][j] = Decimal(amount_in_usd)
                 
    # Calculate the profit
    ABC_in_usd['profit'] = ABC_in_usd[token_address_list].fillna(Decimal(0)).sum(axis=1, min_count=1)
    
    # profit of from_address should minus the gas fee
    if not ABC_in_usd[ABC_in_usd['address'] == from_address].empty: 
        index = ABC_in_usd[ABC_in_usd['address'] == from_address].index[0]   
        from_address_profit = Decimal(ABC_in_usd['profit'][index]) - gas_fee_in_usd
        ABC_in_usd['profit'][index] = from_address_profit
    else:
        from_address_profit = - gas_fee_in_usd
        entry = {'address': from_address, 'profit': from_address_profit}
        ABC_in_usd.loc[len(ABC_in_usd)] = entry
        ABC_in_usd = ABC_in_usd.fillna(Decimal(0))     
        
    # profit of to_address
    if not ABC_in_usd[ABC_in_usd['address'] == to_address].empty: 
        index = ABC_in_usd[ABC_in_usd['address'] == to_address].index[0]   
        to_address_profit = ABC_in_usd['profit'][index]
    else:
        to_address_profit = Decimal(0)
           
    # highest profit address and its profit in usd
    index = ABC_in_usd['profit'].idxmax()
    highest_profit_address = ABC_in_usd['address'][index]
    highest_profit_in_usd = ABC_in_usd['profit'][index]
        
    return ABC_in_usd, price_list, gas_fee, gas_fee_in_usd, from_address_profit, to_address_profit, highest_profit_address, highest_profit_in_usd

def get_flashloan_in_usd(flashloan_token_address, flashloan_amount, date):
    if not token_price[(token_price['token.id'] == flashloan_token_address) & (token_price['date'] == date)].empty:
        index = token_price[(token_price['token.id'] == flashloan_token_address) & (token_price['date'] == date)].index[0]
        price = Decimal(token_price['priceUSD'][index])
    else:
        price = Decimal('NaN')
        if flashloan_token_address not in Balancer_v2_price_cannot_get:
            Balancer_v2_price_cannot_get.append(flashloan_token_address)
    
    flashloan_in_usd = price * Decimal(flashloan_amount)
    return flashloan_in_usd

def process_transaction_with_default_abi(i):
    tx_hash = Balancer_v2_flashloan_unique['tx_hash'][i]
    # Pass current iteration if current tx_hash is marked as error:
    if tx_hash in Balancer_v2_ABC_error_tx_cant_be_solved:
        return
    
    try:    
        block_number = Balancer_v2_flashloan_unique['block_number'][i]
        logIndex = Balancer_v2_flashloan_unique['logIndex'][i]
        transactionIndex = Balancer_v2_flashloan_unique['transactionIndex'][i]
        date = Balancer_v2_flashloan_unique['date'][i]
        from_address = w3.tracing.trace_transaction(tx_hash)[0]['action']['from'].lower()
        to_address = Balancer_v2_flashloan_unique['recipient'][i].lower()


        # flashloan in usd
        flashloan_token_address = Balancer_v2_flashloan_unique['token'][i].lower()
        flashloan_amount = Balancer_v2_flashloan_unique['amount'][i]
        flashloan_in_usd = get_flashloan_in_usd(flashloan_token_address, flashloan_amount, date)
          
        # account balance change
        ABC = AB.get_account_balance_change_full_with_default_abi(tx_hash)
        
        # path_length
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        path_length = len(tx_receipt['logs'])

        # price change ratio
        num_swap_events, reserve_before_swap_list, reserve_after_swap_list, price_change_ratio_list, highest_price_change_ratio = get_price_change_ratio(tx_hash)

        # ABC_simplified    
        ABC_simplified = ABC
        if all(col in ABC_simplified.columns for col in ['ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']):
            ABC_simplified['merged'] = ABC_simplified['ETH'] + ABC_simplified['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']
            ABC_simplified = ABC_simplified.drop(['ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'], axis=1)
            ABC_simplified = ABC_simplified[ABC_simplified.drop('address', axis=1).apply(lambda row: not all(row == 0), axis=1)].reset_index(drop = True)
            ABC_simplified = ABC_simplified.rename(columns={'merged': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'})
            
        # ABC_in_usd
        ABC_in_usd, price_list, gas_fee, gas_fee_in_usd, from_address_profit, to_address_profit, highest_profit_address, highest_profit_in_usd = get_ABC_in_usd(tx_hash, from_address, to_address, ABC_simplified, date)        
                
        # Balancer_v2_ABC
        entry = {'tx_hash': tx_hash , 'date': date, 'block_number': block_number, 'transactionIndex': transactionIndex, 'logIndex': logIndex,
                 'from_address': from_address, 'to_address': to_address, 
                 'account_balance_change': ABC, 'ABC_in_usd': ABC_in_usd, 'price_list': price_list,
                 'gas_fee': gas_fee, 'gas_fee_in_usd': gas_fee_in_usd,
                 'from_address_profit': from_address_profit, 'to_address_profit': to_address_profit,
                 'highest_profit_address': highest_profit_address, 'highest_profit_in_usd': highest_profit_in_usd,
                 'reserve_before_swap_list': reserve_before_swap_list, 'reserve_after_swap_list': reserve_after_swap_list,
                 'price_change_ratio_list': price_change_ratio_list, 'highest_price_change_ratio': highest_price_change_ratio,
                 'path_length': path_length, 'num_swap_events': num_swap_events, 'flashloan_in_usd': flashloan_in_usd
                }
        Balancer_v2_ABC.append(entry)
    except Exception as e:
        if tx_hash not in Balancer_v2_ABC_error_tx_cant_be_solved:
            Balancer_v2_ABC_error_index.append(i)       
        print(f"An error occurred: {e} at index {i}")
        
def process_error_transaction(i):
    try:
        error_index = Balancer_v2_ABC_error_index[i]
        tx_hash = Balancer_v2_flashloan_unique['tx_hash'][error_index]   
        
        block_number = Balancer_v2_flashloan_unique['block_number'][error_index]
        logIndex = Balancer_v2_flashloan_unique['logIndex'][error_index]
        transactionIndex = Balancer_v2_flashloan_unique['transactionIndex'][error_index]
        date = Balancer_v2_flashloan_unique['date'][error_index]
        from_address = w3.tracing.trace_transaction(tx_hash)[0]['action']['from'].lower()
        to_address = Balancer_v2_flashloan_unique['recipient'][error_index].lower()

        # flashloan in usd
        flashloan_token_address = Balancer_v2_flashloan_unique['token'][i].lower()
        flashloan_amount = Balancer_v2_flashloan_unique['amount'][i]
        flashloan_in_usd = get_flashloan_in_usd(flashloan_token_address, flashloan_amount, date)
        
        # account_balance_change  
        ABC = AB.get_account_balance_change_full(tx_hash)

        # path_length
        tx_receipt = w3.eth.get_transaction_receipt(tx_hash)
        path_length = len(tx_receipt['logs'])

        # price change ratio
        num_swap_events, reserve_before_swap_list, reserve_after_swap_list, price_change_ratio_list, highest_price_change_ratio = get_price_change_ratio(tx_hash)

        # ABC_simplified    
        ABC_simplified = ABC
        if all(col in ABC_simplified.columns for col in ['ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']):
            ABC_simplified['merged'] = ABC_simplified['ETH'] + ABC_simplified['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2']
            ABC_simplified = ABC_simplified.drop(['ETH', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'], axis=1)
            ABC_simplified = ABC_simplified[ABC_simplified.drop('address', axis=1).apply(lambda row: not all(row == 0), axis=1)].reset_index(drop = True)
            ABC_simplified = ABC_simplified.rename(columns={'merged': '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'})
            
        # ABC_in_usd
        ABC_in_usd, price_list, gas_fee, gas_fee_in_usd, from_address_profit, to_address_profit, highest_profit_address, highest_profit_in_usd = get_ABC_in_usd(tx_hash, from_address, to_address, ABC_simplified, date)        
                
        # Balancer_v2_ABC
        entry = {'tx_hash': tx_hash , 'date': date, 'block_number': block_number, 'transactionIndex': transactionIndex, 'logIndex': logIndex,
                 'from_address': from_address, 'to_address': to_address, 
                 'account_balance_change': ABC, 'ABC_in_usd': ABC_in_usd, 'price_list': price_list,
                 'gas_fee': gas_fee, 'gas_fee_in_usd': gas_fee_in_usd,
                 'from_address_profit': from_address_profit, 'to_address_profit': to_address_profit,
                 'highest_profit_address': highest_profit_address, 'highest_profit_in_usd': highest_profit_in_usd,
                 'reserve_before_swap_list': reserve_before_swap_list, 'reserve_after_swap_list': reserve_after_swap_list,
                 'price_change_ratio_list': price_change_ratio_list, 'highest_price_change_ratio': highest_price_change_ratio,
                 'path_length': path_length, 'num_swap_events': num_swap_events, 'flashloan_in_usd': flashloan_in_usd
                }
        Balancer_v2_ABC.append(entry)
    except Exception as e:
        if tx_hash not in Balancer_v2_ABC_error_tx_cant_be_solved:
            Balancer_v2_ABC_error_tx_cant_be_solved.append(tx_hash)
        print(f"Error cannot be solved: {e} at {error_index}")

# Process trasactions with default abi
num_processes = 64
with Pool(num_processes) as pool:
    list(tqdm(pool.imap_unordered(process_transaction_with_default_abi, range(len(Balancer_v2_flashloan_unique))),
              desc='Processing transactions with default abi', total=len(Balancer_v2_flashloan_unique)))   
    
# Process the error transactions by getting abi from etherscan
if Balancer_v2_ABC_error_index:
    num_processes = 2
    with Pool(num_processes) as pool:
        list(tqdm(pool.imap_unordered(process_error_transaction, range(len(Balancer_v2_ABC_error_index))),
                  desc='Processing transactions by getting abi from etherscan', total=len(Balancer_v2_ABC_error_index)))

print('initialise')
Balancer_v2_ABC = list(Balancer_v2_ABC)
Balancer_v2_ABC_error_tx_cant_be_solved = list(Balancer_v2_ABC_error_tx_cant_be_solved)
Balancer_v2_price_cannot_get = list(Balancer_v2_price_cannot_get)

print('to df')
Balancer_v2_ABC_df = pd.DataFrame(Balancer_v2_ABC)
Balancer_v2_ABC_error_tx_cant_be_solved_df = pd.DataFrame(Balancer_v2_ABC_error_tx_cant_be_solved)
Balancer_v2_price_cannot_get_df = pd.DataFrame(Balancer_v2_price_cannot_get)

print('to_csv')
Balancer_v2_ABC_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC.csv', index=False)
Balancer_v2_ABC_error_tx_cant_be_solved_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC_error_tx_cant_be_solved.csv', index=False, header = False)
Balancer_v2_price_cannot_get_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_price_cannot_get.csv', index=False, header = False)

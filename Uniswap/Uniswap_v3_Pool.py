from web3 import Web3
import pandas as pd
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#


# V3 找到 contract factory
# 从 contract factory 里面找到 所有的 token pair的 contract address
Uniswap_v3_contract_factory = '0x1F98431c8aD98523631AE4a59f267346ea31F984'
Uniswap_v3_abi = '[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":true,"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"FeeAmountEnabled","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"oldOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnerChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":true,"internalType":"uint24","name":"fee","type":"uint24"},{"indexed":false,"internalType":"int24","name":"tickSpacing","type":"int24"},{"indexed":false,"internalType":"address","name":"pool","type":"address"}],"name":"PoolCreated","type":"event"},{"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"name":"createPool","outputs":[{"internalType":"address","name":"pool","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickSpacing","type":"int24"}],"name":"enableFeeAmount","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint24","name":"","type":"uint24"}],"name":"feeAmountTickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],"name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"parameters","outputs":[{"internalType":"address","name":"factory","type":"address"},{"internalType":"address","name":"token0","type":"address"},{"internalType":"address","name":"token1","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"int24","name":"tickSpacing","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_owner","type":"address"}],"name":"setOwner","outputs":[],"stateMutability":"nonpayable","type":"function"}]'
Uniswap_v3_smart_contract=w3.eth.contract(address=Uniswap_v3_contract_factory,abi=Uniswap_v3_abi)
 
Uniswap_v3_start_block = 12369621
w3_current_block = w3.eth.block_number
step = 50000
Uniswap_v3_PoolCreated_logs = []
Uniswap_v3_PoolCreated_logs_simplified = []

for i in range(Uniswap_v3_start_block, w3_current_block + 1, step):
    Uniswap_v3_PoolCreated_logs = Uniswap_v3_smart_contract.events.PoolCreated().get_logs(fromBlock=i,toBlock=i+step-1)
    for j in range(len(Uniswap_v3_PoolCreated_logs)):
        info = {
            'token0': Uniswap_v3_PoolCreated_logs[j]['args']['token0'],
            'token1': Uniswap_v3_PoolCreated_logs[j]['args']['token1'],
            'fee': Uniswap_v3_PoolCreated_logs[j]['args']['fee'],
            'tickSpacing': Uniswap_v3_PoolCreated_logs[j]['args']['tickSpacing'],
            'pool': Uniswap_v3_PoolCreated_logs[j]['args']['pool'],
            'blockNumber': Uniswap_v3_PoolCreated_logs[j]['blockNumber'],
        }
        Uniswap_v3_PoolCreated_logs_simplified.append(info)

df = pd.DataFrame(Uniswap_v3_PoolCreated_logs_simplified)
df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_Pool.csv', index=False)



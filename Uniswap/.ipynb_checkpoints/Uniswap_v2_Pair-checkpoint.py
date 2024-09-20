from web3 import Web3
import pandas as pd
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#

# V2 找到 contract factory
# 从 contract factory 里面找到 所有的 token pair的 contract address
Uniswap_v2_contract_factory = '0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f'
Uniswap_v2_abi = '[{"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"},{"constant":true,"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"allPairs","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"allPairsLength","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"tokenA","type":"address"},{"internalType":"address","name":"tokenB","type":"address"}],"name":"createPair","outputs":[{"internalType":"address","name":"pair","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"feeTo","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"feeToSetter","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"getPair","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_feeTo","type":"address"}],"name":"setFeeTo","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"_feeToSetter","type":"address"}],"name":"setFeeToSetter","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}]'
Uniswap_v2_smart_contract=w3.eth.contract(address=Uniswap_v2_contract_factory,abi=Uniswap_v2_abi)
 
Uniswap_v2_start_block = 10000835
w3_current_block = w3.eth.block_number
step = 50000
v2_PairCreated_logs = []
v2_PairCreated_logs_simplified = []
v2_Pair = []

for i in range(Uniswap_v2_start_block, w3_current_block + 1, step):
    v2_PairCreated_logs = Uniswap_v2_smart_contract.events.PairCreated().get_logs(fromBlock=i,toBlock=i+step-1)
    for j in range(len(v2_PairCreated_logs)):
        info = {
            'token0': v2_PairCreated_logs[j]['args']['token0'],
            'token1': v2_PairCreated_logs[j]['args']['token1'],
            'pair': v2_PairCreated_logs[j]['args']['pair'],
            'blockNumber': v2_PairCreated_logs[j]['blockNumber'],
        }
        v2_PairCreated_logs_simplified.append(info)
        v2_Pair.append(v2_PairCreated_logs[j]['args']['pair'])
        
df = pd.DataFrame(v2_PairCreated_logs_simplified)
df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/v2_Pair.csv', index=False)
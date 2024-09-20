import pandas as pd

Aave_v2 = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_unique_token.csv')
Aave_v3 = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_unique_token.csv')
Balancer_v2 = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_unique_token.csv')
Uniswap_v2 = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_unique_token.csv')
Uniswap_v3 = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_unique_token.csv')

all_addresses = Aave_v2['address'].to_list() + Aave_v3['address'].to_list() + Balancer_v2['address'].to_list() + Uniswap_v2['address'].to_list() + Uniswap_v3['address'].to_list()

unique_addresses = list(set(all_addresses))

unique_token_df = pd.DataFrame(unique_addresses, columns = ['address'])

unique_token_df.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Unique_token.csv', index=False)
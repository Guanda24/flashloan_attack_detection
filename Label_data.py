import pandas as pd
import ast
from decimal import Decimal
import math

token_price = pd.read_csv('/home/user/gzhao/Thesis/Price/token_price.csv')
Uniswap_v2_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_ABC.csv')
Uniswap_v3_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_ABC.csv')
Aave_v2_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_ABC.csv')
Aave_v3_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_ABC.csv')
Balancer_v2_df = pd.read_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC.csv')
merged_flashloan_df = pd.concat([Uniswap_v2_df, Uniswap_v3_df, Aave_v2_df, Aave_v3_df, Balancer_v2_df], ignore_index=True)

def all_prices_not_nan(price_list_str):
    
    price_list_str = price_list_str.replace("Decimal(", "").replace(")", "")
    price_list = ast.literal_eval(price_list_str)  # Convert string to list of dicts

    for entry in price_list:
        price = float(entry['price'])  # Convert price to float
        if math.isnan(price):  # Check if it is NaN
            return False
    return True  # Return True if no prices are NaN

def check_is_token(address):
    is_in_df = address in token_price['token.id'].values
    return is_in_df


merged_flashloan_filtered_df = merged_flashloan_df[merged_flashloan_df['highest_profit_in_usd'] > 100000]
merged_flashloan_filtered_df = merged_flashloan_filtered_df[~merged_flashloan_filtered_df['highest_profit_address'].apply(check_is_token)].reset_index(drop=True)
merged_flashloan_filtered_df = merged_flashloan_filtered_df[merged_flashloan_filtered_df['price_list'].apply(all_prices_not_nan)].reset_index(drop=True)
attack_label_list = merged_flashloan_filtered_df['tx_hash'].tolist()

# https://www.immunebytes.com/blog/list-of-flash-loan-attacks-in-crypto/
temp = [
"0x3b19e152943f31fe0830b67315ddc89be9a066dc89174256e17bc8c2d35b5af8",
"0xcb0ad9da33ecabf75df0a24aabf8a4517e4a7c5b1b2f11fee3b6a1ad9299a282",
"0xcb58fb952914896b35d909136b9f719b71fc8bc60b59853459fc2476d4369c3a",
"0xf72f1d10fc6923f87279ce6c0aef46e372c6652a696f280b0465a301a92f2e26",
"0x118b7b7c11f9e9bd630ea84ef267b183b34021b667f4a3061f048207d266437a",
]

# https://arxiv.org/abs/2206.10708
temp2 = [
"0x3503253131644dd9f52802d071de74e456570374d586ddd640159cf6fb9b8ad8",
"0x35f8d2f572fceaac9288e5d462117850ef2694786992a8c3f6d02612277b0877",
"0x0fc6d2ca064fc841bc9b1c1fad1fbb97bcea5c9a1b2b66ef837f1227e06519a6",
"0x958236266991bc3fe3b77feaacea120f172c0708ad01c7a715b255f218f9313c",
"0x46a03488247425f845e444b9c10b52ba3c14927c687d38287c0faddc7471150a",
"0x8bb8dc5c7c830bac85fa48acad2505e9300a91c3ff239c9517d0cae33b595090",
"0xf6022012b73770e7e2177129e648980a82aab555f9ac88b8a9cda3ec44b30779",
"0xcd314668aaa9bbfebaf1a0bd2b6553d01dd58899c508d4729fa7311dc5d33ad7"
]


attack_label_list.extend(temp)
attack_label_list.extend(temp2)

unlabeled = merged_flashloan_df[~merged_flashloan_df['tx_hash'].isin(attack_label_list)].reset_index(drop=True)
attack_label = merged_flashloan_df[merged_flashloan_df['tx_hash'].isin(attack_label_list)].reset_index(drop=True)

unlabeled.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/unlabeled.csv', index=False)
attack_label.to_csv('/local/scratch/exported/MP_Defi_txs_TY_23/guanda/attack_label.csv', index=False)
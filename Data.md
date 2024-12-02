# Project Data Overview
This section provides a comprehensive overview of the project's data files. Each file serves a specific purpose, and the following sections provide detailed information about each dataset.

## General

### timestamp_21145534.csv

- **File Name:** timestamp_21145534.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/timestamp_21145534.csv
- **File Type:** pd.DataFrame
- **Column Names:** block_number, timestamp

### Unique_token.csv

- **File Name:** Unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### attack_label.csv

- **File Name:** attack_label.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/attack_label.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### unlabeled.csv

- **File Name:** unlabeled.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/unlabeled.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### token_price.csv

- **File Name:** token_price.csv
- **File Location:** /home/user/gzhao/Thesis/Price/token_price.csv
- **File Type:** pd.DataFrame
- **Column Names:** dailyTxns, dailyVolumeETH, dailyVolumeToken, dailyVolumeUSD, date, id, priceUSD, token.decimals, token.id, token.name, token.symbol

### token_price_filtered.csv

- **File Name:** token_price_filtered.csv
- **File Location:** /home/user/gzhao/Thesis/Price/token_price_filtered.csv
- **File Type:** pd.DataFrame
- **Column Names:** dailyTxns, dailyVolumeETH, dailyVolumeToken, dailyVolumeUSD, date, id, priceUSD, token.decimals, token.id, token.name, token.symbol

## Uniswap_v2

### uni-v2-sender_equal_to_21089068.hdf5

- **File Name:** uni-v2-sender_equal_to_21089068.hdf5
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/uni-v2-sender_equal_to_21089068.hdf5
- **File Type:** vaex.dataframe.DataFrameLocal
- **Column Names:** block_number, timestamp, tx_hash, log_index, tx_index, pair_contract_address, sender, to, amount0_in, amount1_in, amount0_out, amount1_out

### uniswap-v2-swap_drop_duplicates.hdf5

- **File Name:** uniswap-v2-swap_drop_duplicates.hdf5
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v2-swap_drop_duplicates.hdf5
- **File Type:** vaex.dataframe.DataFrameLocal
- **Column Names:** block_number, timestamp, tx_hash, log_index, tx_index, pair_contract_address, sender, to, amount0_in, amount1_in, amount0_out, amount1_out

### uniswap-v2-sync_drop_duplicates.hdf5

- **File Name:** uniswap-v2-sync_drop_duplicates.hdf5
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v2-sync_drop_duplicates.hdf5
- **File Type:** vaex.dataframe.DataFrameLocal
- **Column Names:** block_number, timestamp, tx_hash, log_index, tx_index, pair_contract_address, sender, to, reserve0, reserve1

### Uniswap_v2_flashloan_logs.csv

- **File Name:** Uniswap_v2_flashloan_logs.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_logs.csv
- **File Type:** pd.DataFrame
- **Column Names:** block_number, timestamp, tx_hash, transactionIndex, logIndex, sender, recipient, pair_contract_address, amount0_in	amount1_in, amount0_out, amount1_out, date

### Uniswap_v2_flashloan_unique.csv

- **File Name:** Uniswap_v2_flashloan_unique.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_unique.csv
- **File Type:** pd.DataFrame
- **Column Names:** block_number, timestamp, tx_hash, transactionIndex, logIndex, sender, recipient, pair_contract_address, amount0_in	amount1_in, amount0_out, amount1_out, date	

### Uniswap_v2_flashloan_count.csv

- **File Name:** Uniswap_v2_flashloan_count.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_flashloan_count.csv
- **File Type:** pd.DataFrame
- **Column Names:** date, daily_flash_tx_count

### Uniswap_v2_unique_token.csv

- **File Name:** Uniswap_v2_unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### Uniswap_v2_ABC.csv

- **File Name:** Uniswap_v2_ABC.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_ABC.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### Uniswap_v2_ABC_error_tx_cant_be_solved.csv

- **File Name:** Uniswap_v2_ABC_error_tx_cant_be_solved.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v2_ABC_error_tx_cant_be_solved.csv
- **File Type:** pd.DataFrame
- **Column Names:** no column name

## Uniswap_v3

### uniswap-v3-poolcreated-drop-duplicates.csv

- **File Name:** uniswap-v3-poolcreated-drop-duplicates.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v3-poolcreated-drop-duplicates.csv
- **File Type:** pd.DataFrame
- **Column Names:** block_number, timestamp, tx_hash, log_index, tx_index, factory_contract_address, pool_contract_address, fee, token0_address, token0_symbol, token1_address, token1_symbol

### uniswap-v3-swap-drop-duplicates.hdf5

- **File Name:** uniswap-v3-swap-drop-duplicates.hdf5
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/uniswap-v3-swap-drop-duplicates.hdf5
- **File Type:** vaex.dataframe.DataFrameLocal
- **Column Names:** block_number, timestamp, tx_hash, log_index, tx_index, pair_contract_address, sender, to, amount0, amount1, sqrt_price_x96, liquidity, tick

### Uniswap_v3_flashloan_logs.csv

- **File Name:** Uniswap_v3_flashloan_logs.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_logs.csv
- **File Type:** pd.DataFrame
- **Column Names:** sender, recipient, token0_symbol, token1_symbol, amount0, amount1, paid0, paid1, pair_contract_address, tx_hash, logIndex, transactionIndex, block_number, timestamp, date, token, amount

### Uniswap_v3_flashloan_unique.csv

- **File Name:** Uniswap_v3_flashloan_unique.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_unique.csv
- **File Type:** pd.DataFrame
- **Column Names:** sender, recipient, token0_symbol, token1_symbol, amount0, amount1, paid0, paid1, pair_contract_address, tx_hash, logIndex, transactionIndex, block_number, timestamp, date, token, amount

### Uniswap_v3_flashloan_count.csv

- **File Name:** Uniswap_v3_flashloan_count.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_flashloan_count.csv
- **File Type:** pd.DataFrame
- **Column Names:** date, daily_flash_tx_count

### Uniswap_v3_unique_token.csv

- **File Name:** Uniswap_v3_unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### Uniswap_v3_ABC.csv

- **File Name:** Uniswap_v3_ABC.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_ABC.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### Uniswap_v3_ABC_error_tx_cant_be_solved.csv

- **File Name:** Uniswap_v3_ABC_error_tx_cant_be_solved.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Uniswap_v3_ABC_error_tx_cant_be_solved.csv
- **File Type:** pd.DataFrame
- **Column Names:** no column name

## Aave_v2

### Aave_v2_flashloan_logs.csv

- **File Name:** Aave_v2_flashloan_logs.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_flashloan_logs.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Aave_v2_flashloan_unique.csv

- **File Name:** Aave_v2_flashloan_unique.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_flashloan_unique.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Aave_v2_flashloan_count.csv

- **File Name:** Aave_v2_flashloan_count.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_flashloan_count.csv
- **File Type:** pd.DataFrame
- **Column Names:** date, daily_flash_tx_count

### Aave_v2_unique_token.csv

- **File Name:** Aave_v2_unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### Aave_v2_ABC.csv

- **File Name:** Aave_v2_ABC.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_ABC.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### Aave_v2_ABC_error_tx_cant_be_solved.csv

- **File Name:** Aave_v2_ABC_error_tx_cant_be_solved.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v2_ABC_error_tx_cant_be_solved.csv
- **File Type:** pd.DataFrame
- **Column Names:** no column name

## Aave_v3

### Aave_v3_flashloan_logs.csv

- **File Name:** Aave_v3_flashloan_logs.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_flashloan_logs.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Aave_v3_flashloan_unique.csv

- **File Name:** Aave_v3_flashloan_unique.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_flashloan_unique.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Aave_v3_flashloan_count.csv

- **File Name:** Aave_v3_flashloan_count.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_flashloan_count.csv
- **File Type:** pd.DataFrame
- **Column Names:** date, daily_flash_tx_count

### Aave_v3_unique_token.csv

- **File Name:** Aave_v3_unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### Aave_v3_ABC.csv

- **File Name:** Aave_v3_ABC.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_ABC.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### Aave_v3_ABC_error_tx_cant_be_solved.csv

- **File Name:** Aave_v3_ABC_error_tx_cant_be_solved.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Aave_v3_ABC_error_tx_cant_be_solved.csv
- **File Type:** pd.DataFrame
- **Column Names:** no column name

## Balancer_v2

### Balancer_v2_flashloan_logs.csv

- **File Name:** Balancer_v2_flashloan_logs.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_flashloan_logs.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Balancer_v2_flashloan_unique.csv

- **File Name:** Balancer_v2_flashloan_unique.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_flashloan_unique.csv
- **File Type:** pd.DataFrame
- **Column Names:** recipient, token, amount, fee, tx_hash, logIndex, transactionIndex, block_number, timestamp, date

### Balancer_v2_flashloan_count.csv

- **File Name:** Balancer_v2_flashloan_count.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_flashloan_count.csv
- **File Type:** pd.DataFrame
- **Column Names:** date, daily_flash_tx_count

### Balancer_v2_unique_token.csv

- **File Name:** Balancer_v2_unique_token.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_unique_token.csv
- **File Type:** pd.DataFrame
- **Column Names:** address

### Balancer_v2_ABC.csv

- **File Name:** Balancer_v2_ABC.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC.csv
- **File Type:** pd.DataFrame
- **Column Names:** tx_hash, date, block_number, transactionIndex, log_index, from_address, to_address, account_balance_change, ABC_in_usd, price_list, highest_profit_address, highest_profit_in_usd, sync_before, sync_after, sync_price_ratio, highest_sync_price_ratio, path_length, num_swap_events, flashloan_in_usd

### Balancer_v2_ABC_error_tx_cant_be_solved.csv

- **File Name:** Balancer_v2_ABC_error_tx_cant_be_solved.csv
- **File Location:** /local/scratch/exported/MP_Defi_txs_TY_23/guanda/Balancer_v2_ABC_error_tx_cant_be_solved.csv
- **File Type:** pd.DataFrame
- **Column Names:** no column name
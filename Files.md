# Project Files Overview

This section provides an overview of various project files, each serving a specific purpose. Detailed descriptions of each file and its usage are provided for better understanding.

## General

### Unique_token.py

- **File Name:** Unique_token.py
- **Usage:** Get all unique tokens that have been used in Uniswap V2, Uniswap V3, Aave V2, Aave V3 and Balancer V2 flashloan transactions
- **Data Output:** Unique_token.csv

### Token_price.py

- **File Name:** Token_price.py
- **Usage:** Get all token prices by using GraphQLClient
- **Data Output:** token_price.csv

### Token_Price_check.ipynb

- **File Name:** Token_Price_check.ipynb
- **Usage:** Remove abnormal token prices
- **Data Output:** token_price_filtered.csv

### Supervised_learning.ipynb

- **File Name:** Supervised_learning.ipynb
- **Usage:** Machine learning for detecting flashloan attacks
- **Data Output:** model.pkl, metrics.csv, precision_recall_data.csv, roc_data.csv

## ABC

### ABC_analysis.py

- **File Name:** ABC_analysis.py
- **Usage:** Account Balance Change Analyszer
- **Data Output:** None

## Uniswap_v2

### Uniswap_v2_daily_flashloan.py

- **File Name:** Uniswap_v2_daily_flashloan.py
- **Usage:** Get all Uniswap V2 flashloan transactions
- **Data Output:** Uniswap_v2_flashloan_logs.csv, Uniswap_v2_daily_flashloan_unique.csv, Uniswap_v2_flashloan_count.csv

### Uniswap_v2_flashloan_unique_token.py

- **File Name:** Uniswap_v2_flashloan_unique_token.py
- **Usage:** Get all unique tokens that have been used in Uniswap V2 flashloan transactions
- **Data Output:** Uniswap_v2_unique_token.csv

### Uniswap_v2_ABC.py

- **File Name:** Uniswap_v2_ABC.py
- **Usage:** Get the data for identifying Uniswap V2 flashloan attacks
- **Data Output:** Uniswap_v2_ABC.csv, Uniswap_v2_ABC_error_tx_cant_be_solved.csv

### Uniswap_v2_ABC_multiprocessing.py

- **File Name:** Uniswap_v2_ABC_multiprocessing.py
- **Usage:** Get the data for identifying Uniswap V2 flashloan attacks by using multiprocessing
- **Data Output:** Uniswap_v2_ABC.csv, Uniswap_v2_ABC_error_tx_cant_be_solved.csv

## Uniswap_v3

### Uniswap_v3_daily_flashloan.py

- **File Name:** Uniswap_v3_daily_flashloan.py
- **Usage:** Get all Uniswap V3 flashloan transactions
- **Data Output:** Uniswap_v3_flashloan_logs.csv, Uniswap_v3_daily_flashloan_unique.csv, Uniswap_v3_flashloan_count.csv

### Uniswap_v3_flashloan_unique_token.py

- **File Name:** Uniswap_v3_flashloan_unique_token.py
- **Usage:** Get all unique tokens that have been used in Uniswap V3 flashloan transactions
- **Data Output:** Uniswap_v3_unique_token.csv

### Uniswap_v3_ABC.py

- **File Name:** Uniswap_v3_ABC.py
- **Usage:** Get the data for identifying Uniswap V3 flashloan attacks
- **Data Output:** Uniswap_v3_ABC.csv, Uniswap_v3_ABC_error_tx_cant_be_solved.csv

### Uniswap_v3_ABC_multiprocessing.py

- **File Name:** Uniswap_v3_ABC_multiprocessing.py
- **Usage:** Get the data for identifying Uniswap V3 flashloan attacks by using multiprocessing
- **Data Output:** Uniswap_v3_ABC.csv, Uniswap_v3_ABC_error_tx_cant_be_solved.csv

## Aave_v2

### Aave_v2_daily_flashloan.py

- **File Name:** Aave_v2_daily_flashloan.py
- **Usage:** Get all Aave V2 flashloan transactions
- **Data Output:** Aave_v2_flashloan_logs.csv, Aave_v2_daily_flashloan_unique.csv, Aave_v2_flashloan_count.csv

### Aave_v2_flashloan_unique_token.py

- **File Name:** Aave_v2_flashloan_unique_token.py
- **Usage:** Get all unique tokens that have been used in Aave V2 flashloan transactions
- **Data Output:** Aave_v2_unique_token.csv

### Aave_v2_ABC.py

- **File Name:** Aave_v2_ABC.py
- **Usage:** Get the data for identifying Aave V2 flashloan attacks
- **Data Output:** Aave_v2_ABC.csv, Aave_v2_ABC_error_tx_cant_be_solved.csv

### Aave_v2_ABC_multiprocessing.py

- **File Name:** Aave_v2_ABC_multiprocessing.py
- **Usage:** Get the data for identifying Aave V2 flashloan attacks by using multiprocessing
- **Data Output:** Aave_v2_ABC.csv, Aave_v2_ABC_error_tx_cant_be_solved.csv

## Aave_v3

### Aave_v3_daily_flashloan.py

- **File Name:** Aave_v3_daily_flashloan.py
- **Usage:** Get all Aave V3 flashloan transactions
- **Data Output:** Aave_v3_flashloan_logs.csv, Aave_v3_daily_flashloan_unique.csv, Aave_v3_flashloan_count.csv

### Aave_v3_flashloan_unique_token.py

- **File Name:** Aave_v3_flashloan_unique_token.py
- **Usage:** Get all unique tokens that have been used in Aave V3 flashloan transactions
- **Data Output:** Aave_v3_unique_token.csv

### Aave_v3_ABC.py

- **File Name:** Aave_v3_ABC.py
- **Usage:** Get the data for identifying Aave V3 flashloan attacks
- **Data Output:** Aave_v3_ABC.csv, Aave_v3_ABC_error_tx_cant_be_solved.csv

### Aave_v3_ABC_multiprocessing.py

- **File Name:** Aave_v3_ABC_multiprocessing.py
- **Usage:** Get the data for identifying Aave V3 flashloan attacks by using multiprocessing
- **Data Output:** Aave_v3_ABC.csv, Aave_v3_ABC_error_tx_cant_be_solved.csv

## Balancer_v2

### Balancer_v2_daily_flashloan.py

- **File Name:** Balancer_v2_daily_flashloan.py
- **Usage:** Get all Balancer V2 flashloan transactions
- **Data Output:** Balancer_v2_flashloan_logs.csv, Balancer_v2_daily_flashloan_unique.csv, Balancer_v2_flashloan_count.csv

### Balancer_v2_flashloan_unique_token.py

- **File Name:** Balancer_v2_flashloan_unique_token.py
- **Usage:** Get all unique tokens that have been used in Balancer V2 flashloan transactions
- **Data Output:** Balancer_v2_unique_token.csv

### Balancer_v2_ABC.py

- **File Name:** Aave_v3_ABC.py
- **Usage:** Get the data for identifying Balancer V2 flashloan attacks
- **Data Output:** Balancer_v2_ABC.csv, Balancer_v2_ABC_error_tx_cant_be_solved.csv

### Balancer_v2_ABC_multiprocessing.py

- **File Name:** Aave_v3_ABC_multiprocessing.py
- **Usage:** Get the data for identifying Balancer V2 flashloan attacks by using multiprocessing
- **Data Output:** Balancer_v2_ABC.csv, Balancer_v2_ABC_error_tx_cant_be_solved.csv






## ABC_without_symbol.py

- **File Name:** ABC_without_symbol.py
- **Usage:** Calculate the account balance change of both internal and external transactions without returning the symbols of tokens

## Account_Balance.py

- **File Name:** Account_Balance.py
- **Usage:** Calculate the account balance change of only the external transactions and return the symbols of tokens

## Account_Balance_Full.py

- **File Name:** Account_Balance_Full.py
- **Usage:** Calculate the account balance change of both internal and external transactions and return the symbols of tokens

## find_implement_of_proxy.py

- **File Name:** find_implement_of_proxy.py
- **Usage:** Find the actual address of the proxy contract


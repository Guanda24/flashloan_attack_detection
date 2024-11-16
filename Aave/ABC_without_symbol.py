from web3 import Web3, HTTPProvider
from decimal import Decimal, getcontext
from collections import defaultdict
import pandas as pd
w3 = Web3(Web3.HTTPProvider("http://localhost:8547",request_kwargs={'timeout': 80}))#

getcontext().prec = 50  # Set the precision for Decimal
pd.set_option('display.float_format', lambda x: '%.18f' % x)

# hash of different topics
transfer_topic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
withdrawal_topic = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
deposit_topic = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'

# default abi
default_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]'   

def get_abi_from_etherscan(sc_address):
    #申请一个 etherscan API key
    YourApiKeyToken='8R8P37SDCDX7WFW3B2QZTQS68X59F4QE9I'
    sc_address=sc_address
    import requests
    import json
    result=requests.get(f'https://api.etherscan.io/api?module=contract&action=getabi&address={sc_address}&apikey={YourApiKeyToken}')

    abi=result.json()['result']
    return abi 

def is_proxy_contract(contract_address):
    implementation_slot1='0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3'
    implementation_slot2='0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'
    # 获取合约的存储槽值
    storage_value1 = w3.eth.get_storage_at(contract_address, implementation_slot1)
    implement_address1='0x'+storage_value1.hex()[26:]
    if implement_address1=='0x0000000000000000000000000000000000000000': # try another 
        storage_value2 = w3.eth.get_storage_at(contract_address, implementation_slot2)
        implement_address2 = '0x' + storage_value2.hex()[26:]
        if implement_address2=='0x0000000000000000000000000000000000000000': 
            return None
        else:
            return convert_to_checksum_address(implement_address2)
    else:
        return convert_to_checksum_address(implement_address1)

def convert_to_checksum_address(hex_address):

    normalized_address = '0x' + hex_address[-40:]
    checksummed_address = w3.to_checksum_address(normalized_address)
    
    return checksummed_address

def get_transfer_list_with_default_abi(tx_hash):
    tx_receipt = w3.eth.get_transaction_receipt(tx_hash) #get tx_recipt from the tx_hash
    logs = tx_receipt['logs'] #get logs from the tx_receipt    
    transfer_list = [] #initialise empty transfer_list
    
    for i in range(len(logs)):
        try:
            # there is a rare case that the log did nothing and the topic is empty
            if logs[i]['topics']:
                # transfer topic
                if logs[i]['topics'][0].hex() == transfer_topic:
                    sender = convert_to_checksum_address(logs[i]['topics'][1].hex())
                    receiver = convert_to_checksum_address(logs[i]['topics'][2].hex())
                    if logs[i]['data'].hex() == '0x':
                        balance_change = 0
                        pass
                    else:
                        balance_change = Decimal(int(logs[i]['data'].hex(),16))
                        contract_address = logs[i]['address']         
                        contract = w3.eth.contract(address=contract_address, abi=default_abi)

                        # get token symbol
                        token_symbol = contract_address.lower()

                        # get decimal 
                        decimal = contract.functions.decimals().call()

                        balance_change = balance_change / Decimal(10 ** decimal)

                        if sender == '0x0000000000000000000000000000000000000000': # Burn
                            sender = token_symbol
                        if receiver == '0x0000000000000000000000000000000000000000': # Mint
                            receiver = token_symbol

                        transfer_list.append({'address':sender.lower(), 
                                              'balance_change': -balance_change, 
                                              'token_symbol': token_symbol
                                             })
                        transfer_list.append({'address':receiver.lower(), 
                                              'balance_change': balance_change, 
                                              'token_symbol': token_symbol
                                             })

                # Withdrawal topic
                elif logs[i]['topics'][0].hex() == withdrawal_topic:
                    sender = logs[i]['address']
                    receiver = convert_to_checksum_address(logs[i]['topics'][1].hex())
                    balance_change = Decimal(int(logs[i]['data'].hex(),16))
                    contract_address = logs[i]['address']
                    contract = w3.eth.contract(address=contract_address, abi=default_abi)

                    # get token symbol
                    token_symbol = contract_address.lower() 

                    # get decimal 
                    decimal = contract.functions.decimals().call()

                    balance_change = balance_change / Decimal(10 ** decimal)
                    transfer_list.append({'address':sender.lower(),
                                          'balance_change': balance_change,
                                          'token_symbol': token_symbol
                                         })
                    transfer_list.append({'address':receiver.lower(),  
                                          'balance_change': -balance_change, 
                                          'token_symbol': token_symbol
                                         })

                # Deposit topic
                elif logs[i]['topics'][0].hex() == deposit_topic:
                    sender = logs[i]['address']
                    receiver = convert_to_checksum_address(logs[i]['topics'][1].hex())
                    balance_change = Decimal(int(logs[i]['data'].hex(),16))
                    contract_address = logs[i]['address']
                    contract = w3.eth.contract(address=contract_address, abi=default_abi)

                    # get token symbol
                    token_symbol = contract_address.lower() 

                    # get decimal 
                    decimal = contract.functions.decimals().call()

                    balance_change = balance_change / Decimal(10 ** decimal)
                    transfer_list.append({'address':sender.lower(),
                                          'balance_change': -balance_change,
                                          'token_symbol': token_symbol
                                         })
                    transfer_list.append({'address':receiver.lower(),  
                                          'balance_change': balance_change,
                                          'token_symbol': token_symbol
                                         })  
        except Exception as e:
            # Handle exception or log error if needed
            pass
            
    return transfer_list

def get_account_balance_change_from_transfer_list_with_default_abi(transfer_list):  
    # Create a defaultdict to store balance changes for each address and token_symbol
    balance_changes = defaultdict(dict)

    # Iterate through the list and update the balance_changes dictionary
    for entry in transfer_list:
        address = entry['address']
        token_symbol = entry['token_symbol']
        balance_change = entry['balance_change']

        # Update the balance for the corresponding address and token symbol
        if token_symbol not in balance_changes[address]:
            balance_changes[address][token_symbol] = balance_change
        else:
            balance_changes[address][token_symbol] += balance_change

    # Convert the defaultdict to a list of dictionaries for DataFrame creation
    result_list = [{'address': address, **balances} for address, balances in balance_changes.items()]

    # Create a DataFrame from the list
    result_df = pd.DataFrame(result_list).fillna(0)

    return result_df


def get_account_balance_change_with_default_abi(tx):
    transfer_list = get_transfer_list_with_default_abi(tx)
    df = get_account_balance_change_from_transfer_list_with_default_abi(transfer_list)
    return df


def get_transfer_list(tx_hash):
    tx_receipt = w3.eth.get_transaction_receipt(tx_hash) #get tx_recipt from the tx_hash
    logs = tx_receipt['logs'] #get logs from the tx_receipt
    
    transfer_list = []
    for i in range(len(logs)):
        try:
            # transfer topic
            if logs[i]['topics'][0].hex() == transfer_topic:
                sender = convert_to_checksum_address(logs[i]['topics'][1].hex())
                receiver = convert_to_checksum_address(logs[i]['topics'][2].hex())
                if logs[i]['data'].hex() == '0x':
                    balance_change = 0
                    pass
                else:
                    balance_change = Decimal(int(logs[i]['data'].hex(),16))
                    contract_address = logs[i]['address']

                    if is_proxy_contract(contract_address) is None:
                        abi = get_abi_from_etherscan(contract_address)
                    else:
                        abi = get_abi_from_etherscan(is_proxy_contract(contract_address))

                    contract = w3.eth.contract(address=contract_address, abi=abi)    

                    decimal = contract.functions.decimals().call()           

                    token_symbol = contract_address.lower()  
                    balance_change = balance_change / Decimal(10 ** decimal)
                    transfer_list.append({'address':sender.lower(), 
                                          'balance_change': -balance_change,
                                          'token_symbol': token_symbol
                                         })
                    transfer_list.append({'address':receiver.lower(), 
                                          'balance_change': balance_change, 
                                          'token_symbol': token_symbol
                                         })


            # Withdrawal topic
            elif logs[i]['topics'][0].hex() == withdrawal_topic:
                sender = logs[i]['address']
                receiver = convert_to_checksum_address(logs[i]['topics'][1].hex())
                balance_change = Decimal(int(logs[i]['data'].hex(),16))
                contract_address = logs[i]['address']
                if is_proxy_contract(contract_address) is None:
                    abi = get_abi_from_etherscan(contract_address)
                else:
                    abi = get_abi_from_etherscan(is_proxy_contract(contract_address)) 

                contract = w3.eth.contract(address=contract_address, abi=abi)    

                decimal = contract.functions.decimals().call()           

                token_symbol = contract_address.lower()   
                balance_change = balance_change / Decimal(10 ** decimal)
                transfer_list.append({'address':sender.lower(),
                                      'balance_change': balance_change, 
                                      'token_symbol': token_symbol
                                     })
                transfer_list.append({'address':receiver.lower(),
                                      'balance_change': -balance_change, 
                                      'token_symbol': token_symbol
                                     })


            # Deposit topic
            elif logs[i]['topics'][0].hex() == deposit_topic:
                sender = logs[i]['address']
                receiver = convert_to_checksum_address(logs[i]['topics'][1].hex())
                balance_change = Decimal(int(logs[i]['data'].hex(),16))
                contract_address = logs[i]['address']    

                if is_proxy_contract(contract_address) is None:
                    abi = get_abi_from_etherscan(contract_address)
                else: 
                    abi = get_abi_from_etherscan(is_proxy_contract(contract_address)) 

                contract = w3.eth.contract(address=contract_address, abi=abi)    

                decimal = contract.functions.decimals().call()           

                token_symbol = contract_address.lower()  
                balance_change = balance_change / Decimal(10 ** decimal)
                transfer_list.append({'address':sender.lower(),
                                      'balance_change': -balance_change,
                                      'token_symbol': token_symbol
                                     })
                transfer_list.append({'address':receiver.lower(),
                                      'balance_change': balance_change, 
                                      'token_symbol': token_symbol
                                     })
        except Exception as e:
            # Handle exception or log error if needed
            pass

    return transfer_list

def get_account_balance_change_from_transfer_list(transfer_list):  
    # Create a defaultdict to store balance changes for each address and token_symbol
    balance_changes = defaultdict(dict)


    # Iterate through the list and update the balance_changes dictionary
    for entry in transfer_list:
        address = entry['address']
        token_symbol = entry['token_symbol']
        balance_change = entry['balance_change']

        # Update the balance for the corresponding address and token symbol
        if token_symbol not in balance_changes[address]:
            balance_changes[address][token_symbol] = balance_change
        else:
            balance_changes[address][token_symbol] += balance_change

    # Convert the defaultdict to a list of dictionaries for DataFrame creation
    result_list = [{'address': address, **balances} for address, balances in balance_changes.items()]

    # Create a DataFrame from the list
    result_df = pd.DataFrame(result_list).fillna(0)

    return result_df


def get_account_balance_change(tx):
    transfer_list = get_transfer_list(tx)
    df = get_account_balance_change_from_transfer_list(transfer_list)
    return df

# Wanke - internal and external ETH    

def keep_eth_decimal(x):
    decimal_value = Decimal(x)
    return decimal_value / 10**18

def hex_to_int(x):
    if isinstance(x, str):
        # Check if the value is a hexadecimal string
        if x.startswith('0x'):
            return int(x, 16)
        else:
            return int(x)
    else:
        # Convert non-string input to string and then to integer
        return int(str(x), 16)

def convert_to_decimal(value):
    # Check if the value starts with '0x' (indicating it's a hexadecimal string)
    if isinstance(value, str) and value.startswith('0x'):
        return int(value, 16)
    else:
        return value

def analyze_transaction(tx_hash):
    # 设置与节点的连接
    w3 = HTTPProvider('http://localhost:8547', request_kwargs={'timeout': 60})

    # 获取交易追踪数据
    result = w3.make_request('trace_replayTransaction', [tx_hash, ['trace']])

    # 内部交易数据转换
    internal_txs=pd.json_normalize(result['result']['trace'])

    tx=Web3(w3).eth.get_transaction(tx_hash)

    internal_txs = internal_txs[pd.notna(internal_txs['action.value'])]
    
    # 计算地址的资金变动
    internal_txs['action.value'] = internal_txs['action.value'].apply(hex_to_int)
    internal_txs['action.value'] = internal_txs['action.value'].apply(convert_to_decimal)
    internal_txs['action.value'] = internal_txs['action.value'].apply(keep_eth_decimal)

    internal_txs.drop(internal_txs[internal_txs['action.callType'] == 'delegatecall'].index, inplace=True)

    df = internal_txs
    df['action.value'] = df['action.value'].apply(lambda x: '{:.18f}'.format(x))
    df['action.value'] = df['action.value'].astype(float)
    #df = df[df['action.value'].notna() & (df['action.value'] != 0)]

    #把数值型的attribute改成十进制；
    #value >0 的交易
    if 'action.from' not in internal_txs.columns:
        internal_txs['action.from'] = "Missing"

    # Check if 'action.to' is missing
    if 'action.to' not in internal_txs.columns:
        internal_txs['action.to'] = "Missing"
    
    if df['action.value'].empty or df['action.to'].empty:
        print("df_value is empty. Exiting.")
        return 0
    else:
        df_out = df.groupby('action.from')['action.value'].sum().reset_index()
        df_out.columns = ['account', 'change']
        df_out['change'] = -df_out['change']
        df_out = df_out[df_out['change'] != 0]

        df_in = df.groupby("action.to")['action.value'].sum().reset_index()
        df_in.columns = ['account', 'change']
        df_in = df_in[df_in['change'] != 0]

        # 合并转出和转入的结果，并对相同账户的变化求和
        df_combined = pd.concat([df_out, df_in], ignore_index=True)
        df_result = df_combined.groupby('account')['change'].sum().reset_index()
        df_result = df_result[df_result['change'] != 0]

        # 第一个字典
        result_df = df_result

        # 第二个字典
        transaction_data = tx

        # 提取 from、to 和 value
        from_address = transaction_data['from']
        to_address = transaction_data['to']
        value = transaction_data['value']

        # 检查 from_address 是否在第一个字典中
        if from_address in result_df['account'].values:
            # 找到匹配的行
            row_index = result_df.index[result_df['account'] == from_address][0]
            # 更新账户变化
            result_df.at[row_index, 'change'] -= value
        else:
            # 如果不在，创建新行
            new_row = {'account': from_address, 'change': -value}
            # 将新行添加到 DataFrame
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)

        # 检查 to_address 是否在第一个字典中
        if to_address in result_df['account'].values:
            # 找到匹配的行
            row_index = result_df.index[result_df['account'] == to_address][0]
            # 更新账户变化
            result_df.at[row_index, 'change'] += value
        else:
            # 如果不在，创建新行
            new_row = {'account': to_address, 'change': value}
            # 将新行添加到 DataFrame
            result_df = pd.concat([result_df, pd.DataFrame([new_row])], ignore_index=True)



        return df_result

def get_account_balance_change_full(tx_hash):
    # Call get_account_balance_change function
    df_balance_change = get_account_balance_change(tx_hash)

    # Call analyze_transaction function
    df_transaction = analyze_transaction(tx_hash)                      

    df_transaction = df_transaction.rename(columns={'change': 'ETH'})
    df_transaction = df_transaction.rename(columns={'account': 'address'})

    # Merge the two DataFrames on the 'address' and 'account' columns
    if df_balance_change.empty and df_transaction.empty:
        print(f"For the tx_hash {tx_hash}, no transactions were generated.")
    elif df_balance_change.empty:
        return df_transaction
    elif df_transaction.empty:
        return df_balance_change
    else:
        df_combined = pd.merge(df_balance_change, df_transaction, how='outer', left_on='address', right_on='address')
        df_combined.fillna(0, inplace=True)
        df_combined['ETH'] = df_combined['ETH'].apply(lambda x: '{:.18f}'.format(x))
        df_combined['ETH'] = df_combined['ETH'].apply(Decimal)
        df_combined = df_combined[df_combined.drop('address', axis=1).apply(lambda row: not all(row == 0), axis=1)].reset_index(drop = True)

        return df_combined

def get_account_balance_change_full_with_default_abi(tx_hash):
    # Call get_account_balance_change_with_default_abi function
    df_balance_change = get_account_balance_change_with_default_abi(tx_hash)

    # Call analyze_transaction function
    df_transaction = analyze_transaction(tx_hash)                      

    df_transaction = df_transaction.rename(columns={'change': 'ETH'})
    df_transaction = df_transaction.rename(columns={'account': 'address'})

    # Merge the two DataFrames on the 'address' and 'account' columns
    if df_balance_change.empty and df_transaction.empty:
        print(f"For the tx_hash {tx_hash}, no transactions were generated.")
    elif df_balance_change.empty:
        return df_transaction
    elif df_transaction.empty:
        return df_balance_change
    else:
        df_combined = pd.merge(df_balance_change, df_transaction, how='outer', left_on='address', right_on='address')
        df_combined.fillna(0, inplace=True)
        df_combined['ETH'] = df_combined['ETH'].apply(lambda x: '{:.18f}'.format(x))
        df_combined['ETH'] = df_combined['ETH'].apply(Decimal)
        df_combined = df_combined[df_combined.drop('address', axis=1).apply(lambda row: not all(row == 0), axis=1)].reset_index(drop = True)
            
        return df_combined

# Example usage
#tx_hash = '0x191aeb75d5ac81c46d97916cb88f9f02b4a6e5f854823beae99dbd6336f18928'
#result_df = get_account_balance_change_full(tx_hash)
#result_df

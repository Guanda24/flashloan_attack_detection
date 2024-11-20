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
symbol_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]'  

unit256_abi = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"stop","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"owner_","type":"address"}],"name":"setOwner","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"name_","type":"bytes32"}],"name":"setName","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"stopped","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"authority_","type":"address"}],"name":"setAuthority","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"burn","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"push","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"move","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"start","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"authority","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"src","type":"address"},{"name":"guy","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"wad","type":"uint256"}],"name":"pull","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[{"name":"symbol_","type":"bytes32"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"authority","type":"address"}],"name":"LogSetAuthority","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"}],"name":"LogSetOwner","type":"event"},{"anonymous":true,"inputs":[{"indexed":true,"name":"sig","type":"bytes4"},{"indexed":true,"name":"guy","type":"address"},{"indexed":true,"name":"foo","type":"bytes32"},{"indexed":true,"name":"bar","type":"bytes32"},{"indexed":false,"name":"wad","type":"uint256"},{"indexed":false,"name":"fax","type":"bytes"}],"name":"LogNote","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}]'


# contract addresses without symbol() function
contract_addresses_without_symbol = ['0xE0B7927c4aF23765Cb51314A0E0521A9645F0E2A', # DigixDAO
                                    '0xB9A824e6dC289c57fAc91c16C77E37666cCE20e5', #SmartToken
                                     '0xEB9951021698B42e4399f9cBb6267Aa35F82D59D', #LIF
                                     '0x5d6446880FCD004c851EA8920a628c70Ca101117', # EthUnsiwapPCVDeposit
                                     '0x0Ba45A8b5d5575935B8158a88C631E9F9C95a2e5', # TT
                                     '0x75eb894795Ca5C27a73718B29fE2d9e27a74161e', # XYZToken
                                     '0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A', # Curve.fi
                                     '0x0000000000085d4780B73119b644AE5ecd22b376', # TUSD
                                     '0xb4d0C929cD3A1FbDc6d57E7D3315cF0C4d6B4bFa' # Swerve.fi
                                    ]
symbol_of_contract_address_without_symbol = ['DigixDAO',
                                           'SmartToken',
                                            'LIF',
                                            '0x5d6446880FCD004c851EA8920a628c70Ca101117',
                                            'TT',
                                            '0x75eb894795Ca5C27a73718B29fE2d9e27a74161e',
                                            '0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A',
                                             'TUSD',
                                             '0xb4d0C929cD3A1FbDc6d57E7D3315cF0C4d6B4bFa'
                                            ]
decimal_of_contract_address_without_symbol = [0, # DigixDAO
                                              0, #SmartToken
                                              18, #LIF
                                              0, # EthUnsiwapPCVDeposit
                                              0, # TT
                                              0, # XYZToken
                                              0, # Curve.fi
                                              18, # TUSD
                                              0 # Swerve.fi
                                             ]

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

                    # if is_proxy_contract(contract_address) is not None:
                    #     raise ValueError('You should use get_abi_from_etherscan()')
                    
                    if sender == '0x0000000000000000000000000000000000000000': # Burn
                        sender = contract_address.lower()
                    if receiver == '0x0000000000000000000000000000000000000000': # Mint
                        receiver = contract_address.lower()

                    if contract_address in contract_addresses_without_symbol:
                        index = contract_addresses_without_symbol.index(contract_address)
                        token_symbol = symbol_of_contract_address_without_symbol[index]
                        decimal = decimal_of_contract_address_without_symbol[index]
                    else:
                        # get token symbol
                        try:
                            abi = symbol_abi
                            contract = w3.eth.contract(address=contract_address, abi=abi)
                            token_symbol = contract.functions.symbol().call()
                        except Exception as e:
                            if str(e) == 'Python int too large to convert to C ssize_t':
                                abi = unit256_abi
                                contract = w3.eth.contract(address=contract_address, abi=abi)
                                token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
                            else:
                                raise RuntimeError(f"An error occurred: {e}")

                        # get decimal 
                        try:
                            decimal = contract.functions.decimals().call()
                        except Exception as e: 
                            decimal = 0     

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

                # if is_proxy_contract(contract_address) is not None:
                #         raise ValueError('You should use get_abi_from_etherscan()')

                if contract_address in contract_addresses_without_symbol:
                    index = contract_addresses_without_symbol.index(contract_address)
                    token_symbol = symbol_of_contract_address_without_symbol[index]
                    decimal = decimal_of_contract_address_without_symbol[index]
                else:              
                    # get token symbol
                    try:
                        abi = symbol_abi
                        contract = w3.eth.contract(address=contract_address, abi=abi)
                        token_symbol = contract.functions.symbol().call()
                    except Exception as e:
                        if str(e) == 'Python int too large to convert to C ssize_t':
                            abi = unit256_abi
                            contract = w3.eth.contract(address=contract_address, abi=abi)
                            token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
                        else:
                            raise RuntimeError(f"An error occurred: {e}")

                    # get decimal
                    try:
                        decimal = contract.functions.decimals().call()
                    except Exception as e: 
                        decimal = 0

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

                # if is_proxy_contract(contract_address) is not None:
                #         raise ValueError('You should use get_abi_from_etherscan()')

                if contract_address in contract_addresses_without_symbol:
                    index = contract_addresses_without_symbol.index(contract_address)
                    token_symbol = symbol_of_contract_address_without_symbol[index]
                    decimal = decimal_of_contract_address_without_symbol[index]
                else: 
                    # get token symbol
                    try:
                        abi = symbol_abi
                        contract = w3.eth.contract(address=contract_address, abi=abi)
                        token_symbol = contract.functions.symbol().call()
                    except Exception as e:
                        if str(e) == 'Python int too large to convert to C ssize_t':
                            abi = unit256_abi
                            contract = w3.eth.contract(address=contract_address, abi=abi)
                            token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
                        else:
                            raise RuntimeError(f"An error occurred: {e}")

                    # get decimal
                    try:
                        decimal = contract.functions.decimals().call()
                    except Exception as e: 
                        decimal = 0

                balance_change = balance_change / Decimal(10 ** decimal)

                transfer_list.append({'address':sender.lower(),
                                      'balance_change': -balance_change, 
                                      'token_symbol': token_symbol
                                     })
                transfer_list.append({'address':receiver.lower(),
                                      'balance_change': balance_change, 
                                      'token_symbol': token_symbol
                                     })
            
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
                
#                 if contract_address in contract_addresses_without_symbol:
#                     index = contract_addresses_without_symbol.index(contract_address)
#                     token_symbol = symbol_of_contract_address_without_symbol[index]
#                     decimal = decimal_of_contract_address_without_symbol[index]
#                 else:              
#                     if isinstance(contract.functions.symbol().call(), str):
#                         token_symbol = contract.functions.symbol().call()
#                     else:
#                         token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
                        
#                 try:
#                     decimal = contract.functions.decimals().call()
#                 except Exception as e: 
#                     decimal = 0

                # get token symbol
                try:             
                    if isinstance(contract.functions.symbol().call(), str):
                        token_symbol = contract.functions.symbol().call()
                    else:
                        token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
                except Exception as e:
                    token_symbol = contract_address

                # get decimal
                try:
                    decimal = contract.functions.decimals().call()
                except Exception as e: 
                    decimal = 0     
                    
                balance_change = balance_change / Decimal(10 ** decimal)
                
                transfer_list.append(AttributeDict({'address':sender.lower(),  
                                                    'balance_change': -balance_change, 
                                                    'token_symbol': token_symbol
                                                    }))
                transfer_list.append(AttributeDict({'address':receiver.lower(),  
                                                    'balance_change': balance_change, 
                                                    'token_symbol': token_symbol
                                                    }))
            
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
            
            # get token symbol
            try:             
                if isinstance(contract.functions.symbol().call(), str):
                    token_symbol = contract.functions.symbol().call()
                else:
                    token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
            except Exception as e:
                token_symbol = contract_address
                
            # get decimal
            try:
                decimal = contract.functions.decimals().call()
            except Exception as e: 
                decimal = 0     
                
            balance_change = balance_change / Decimal(10 ** decimal)     
            
            transfer_list.append(AttributeDict({'address':sender.lower(),  
                                                'balance_change': balance_change, 
                                                'token_symbol': token_symbol
                                                }))
            transfer_list.append(AttributeDict({'address':receiver.lower(),  
                                                'balance_change': -balance_change, 
                                                'token_symbol': token_symbol
                                                }))
            
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
            
            # if contract_address in contract_addresses_without_symbol:
            #     index = contract_addresses_without_symbol.index(contract_address)
            #     token_symbol = symbol_of_contract_address_without_symbol[index]
            #     decimal = 0
            # else:              
            #     if isinstance(contract.functions.symbol().call(), str):
            #         token_symbol = contract.functions.symbol().call()
            #     else:
            #         token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
            #     decimal = contract.functions.decimals().call()
            
            
            # get token symbol
            try:             
                if isinstance(contract.functions.symbol().call(), str):
                    token_symbol = contract.functions.symbol().call()
                else:
                    token_symbol = contract.functions.symbol().call().decode('utf-8').rstrip('\x00') 
            except Exception as e:
                token_symbol = contract_address
                
            # get decimal
            try:
                decimal = contract.functions.decimals().call()
            except Exception as e: 
                decimal = 0  
                
            balance_change = balance_change / Decimal(10 ** decimal)
            
            transfer_list.append(AttributeDict({'address':sender.lower(),  
                                                'balance_change': -balance_change, 
                                                'token_symbol': token_symbol
                                                }))
            transfer_list.append(AttributeDict({'address':receiver.lower(),  
                                                'balance_change': balance_change, 
                                                'token_symbol': token_symbol
                                                }))

            
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
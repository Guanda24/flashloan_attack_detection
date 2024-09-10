import json
from graphqlclient import GraphQLClient
import pandas as pd
import os.path
from time import sleep

yourapikey_on_subgraph=''
client = GraphQLClient('https://gateway-arbitrum.network.thegraph.com/api/{yourapikey_on_subgraph}/subgraphs/id/EYCKATKGBKLWvSfwvBjzfCBmGwYNdVkduYXVivCsLRFu')

#new 2024-09-10
query = '''
{
  tokenDayDatas(
    first: 1000
    orderBy: date
    orderDirection: asc
    where: {date_gt: start_time, date_lte: end_time, token: "token_id_lower"}
  ) {
    date
    priceUSD
    dailyTxns
    dailyVolumeETH
    dailyVolumeToken
    dailyVolumeUSD
    id
    token {
      decimals
      id
      name
      symbol
    }
  }
}
'''
def collect_batch_data(token_id,data_name,query,start_time,end_time,save_path):
    new_query = query.replace('token_id_lower',token_id).replace('start_time',str(start_time)).replace('end_time',str(end_time))
    while True:
        result = client.execute(new_query)
        data = json.loads(result)
        try:
            liq_calls = data["data"][f"{data_name}"]
        except Exception as e:
            print('error',e)
        else:
            df=pd.json_normalize(liq_calls) 
            df.to_csv(f'{save_path+data_name}_1.csv',mode='a',index=False,header=not os.path.exists(f'{save_path+data_name}.csv'))
            # print('saved')
            if liq_calls==[]:
                break
            else:
                new_start_time=liq_calls[-1]['date']
                new_query = new_query.replace(f'{start_time}',str(new_start_time))
                print(new_start_time)
                print(len(liq_calls))
                if len(liq_calls) < 1000:
                    break

from tqdm import tqdm
#read your token list
# df=pd.read_csv('/srv/abacus-3/Ethereum_token_txs_data/uniswapv3/price/top_50_price/top_50_token.csv')
token_address=df['id'].tolist()
# token_address=['0xa47c8bf37f92aBed4A126BDA807A7b7498661acD'.lower()]
save_path = './' #change to your path
start_time=1588464000
end_time=1706918400
for token_id in tqdm(token_address):
    # Tuesday, 3 May 2020 00:00:00 to Saturday, 3 February 2024 00:00:00
    collect_batch_data(token_id,'tokenDayDatas',query, start_time, end_time, save_path)
    sleep(2)
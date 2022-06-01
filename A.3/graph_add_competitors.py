import json
import time
import re
import pandas as pd

# local imports
from age_utils import create_node_query, create_edge_query, node_set_query, conn, graph_init

set_queries = []
edge_queries = []
lbl = 'Company'
GraphDBname = 'tesla5forces'

df_competitor = pd.read_csv('data_in/tsla_competitor_info.csv')


def process_df_row(row):
    comp_dict = {
        'company_name': row['COMPANY NAME'],
        'ticker': row['TICKER'] ,
        'market_share': row['MARKET SHARE'] 
        }
    comp_dict['market_share'] = comp_dict['market_share'].replace('%','')
    if comp_dict['company_name'] != 'Tesla Inc':
        # node update/create 
        set_queries.append(
            node_set_query(lbl, 'name', comp_dict['company_name'], comp_dict)
            )
        # connect competitors to tesla     lblA,lblB,filterAkey,filterA,filterBkey,filterB,relType
        edge_queries.append(
            create_edge_query(
            lbl,lbl,'name',comp_dict['company_name'],'name','Tesla Inc','COMPETES_WITH'
            ))

for i, j in df_competitor.iterrows():
    process_df_row(j)

set_queries = list(set(set_queries)) # deduplicate the queries simpler than more complex query creation (1 make per model) above
edge_queries = list(set(edge_queries))

#-----------------------------------------------------------------
#------------------Connect to DB------------------------------------

with conn.cursor() as cursor:
    graph_init(cursor, GraphDBname)

    print('pausing...give you a chance to quit if errors reported')
    time.sleep(5)

    print('merging nodes')
    for query in set_queries:
        cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
    print('creating edges')
    for query in edge_queries:
        cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
    conn.commit()

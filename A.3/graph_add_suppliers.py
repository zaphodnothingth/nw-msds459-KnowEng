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

df_suppliers = pd.read_csv('data_in/tsla_supplier_info.csv')


def process_df_row(row):
    comp_dict = {
        'company_name': row['COMPANY NAME'],
        'ticker': row['TICKER'] ,
        'market_cap_b': row['MARKET CAP (B)'] ,
        'revenues_m': row['REVENUES 1yr 3-22 (M)'] ,
        'income_m': row['INCOME 1yr 3-22 (M)'] ,
        'assets_m': row['ASSETS (M)'] 
        }
    
    # node update/create 
    set_queries.append(
        node_set_query(lbl, 'name', comp_dict['company_name'], comp_dict)
        )
    # connect supplier to tesla     lblA,lblB,filterAkey,filterA,filterBkey,filterB,relType
    edge_queries.append(
        create_edge_query(
        lbl,lbl,'name',comp_dict['company_name'],'name','Tesla Inc','SUPPLIES_TO'
        ))

for i, j in df_suppliers.iterrows():
    process_df_row(j)

set_queries = list(set(set_queries)) # deduplicate the queries simpler than more complex query creation (1 make per model) above
edge_queries = list(set(edge_queries))

#-----------------------------------------------------------------
#------------------Connect to DB------------------------------------

with conn.cursor() as cursor:
    try:
        graph_init(cursor, GraphDBname)

        print('pausing...give you a chance to quit if errors reported')
        time.sleep(5)

        for query in set_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        for query in edge_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        conn.commit()
    except Exception as ex:
        print(type(ex), ex)
        # if exception occurs, you must rollback all transaction. 
        conn.rollback()

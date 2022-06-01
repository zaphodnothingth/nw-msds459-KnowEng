import json
import time
import re

# local imports
from age_utils import create_node_query, create_edge_query, node_set_query, conn, graph_init

node_queries = []
set_queries = []
edge_queries = []
lbl = 'Company'
GraphDBname = 'tesla5forces'

with open('data_in/companies.jsonl', 'r') as f:
    next(f)  # skip first line
    for line in f:
        data = json.loads(line)
        company = data['company_name']
        location = data['hq_loc']
        parent = data['parent_company']

        # node creation for make (company)
        node_queries.append(create_node_query('Make', {'name':company}))
        # connect competitors to tesla     lblA,lblB,filterAkey,filterA,filterBkey,filterB,relType
        edge_queries.append(
            create_edge_query(
            'Make',lbl,'name',company,'name','Tesla Inc','COMPETES_WITH'
            ))

        # node creation for parent company
        node_queries.append(create_node_query(lbl, {'name':parent}))
        # connect parent to tesla     lblA,lblB,filterAkey,filterA,filterBkey,filterB,relType
        edge_queries.append(
            create_edge_query(
            lbl,lbl,'name',parent,'name',company,'PARENT_OF'
            ))

        # node creation for LOCATION
        node_queries.append(create_node_query('Location', {'name':location}))
        # connect company to location
        edge_queries.append(
            create_edge_query(lbl,'Location','name',company,'name',location,'HQ_IN')
            )

node_queries = list(set(node_queries)) # deduplicate the queries simpler than more complex query creation (1 make per model) above
edge_queries = list(set(edge_queries))

#-----------------------------------------------------------------
#------------------Connect to DB------------------------------------

with conn.cursor() as cursor:
    try:
        graph_init(cursor, GraphDBname)

        print('pausing...give you a chance to quit if errors reported')
        time.sleep(5)

        print('merging nodes')
        for query in node_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        for query in edge_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        conn.commit()
    except Exception as ex:
        print(type(ex), ex)
        # if exception occurs, you must rollback all transaction. 
        conn.rollback()

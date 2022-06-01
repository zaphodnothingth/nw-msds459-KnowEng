import json
import time
import re

# local imports
from age_utils import create_node_query, create_edge_query, conn, graph_init

node_queries = []
edge_queries = []
lbl = 'Model'

with open('data_out/vehicle_models.jsonl', 'r') as f:
    for line in f:
        data = json.loads(line)
        model = data['Model']
        make = data['Make']
        yr = data['Year']
        categories = data['Category'].split(',')
        categories = [item.replace(' ','') for item in categories]
        categories = list(set([re.sub('\d+','',item) for item in categories])) # remove year from different SUV types and dedup

        # node creation for model
        create_node_query(lbl, {'name':model})
        # node creation for make
        create_node_query('Make', {'name':make})
        # connect model to make
        create_edge_query(lbl,'Make','name',model,'name',make,'MANUFACTURED_BY')

        # node creation for year
        create_node_query('Year', {'int':yr})
        # connect model to year
        create_edge_query(lbl,'Year','name',model,'int',yr,'AVAILABLE_IN')

        for cat in categories:
            # node creation for category
            create_node_query('Category', {'name':cat})
            # connect model to category
            create_edge_query(lbl,'Category','name',model,'name',cat,'CATEGORIZED_AS')


node_queries = list(set(node_queries)) # deduplicate the queries simpler than more complex query creation (1 make per model) above
edge_queries = list(set(edge_queries))

#-----------------------------------------------------------------
#------------------Connect to DB------------------------------------

with conn.cursor() as cursor:
    try:

        #Test the connection and cursor by selecting the current_date from the postgreSQL server
        cursor.execute('SELECT CURRENT_DATE;')
        print("If your connection worked the current date is:")
        print(cursor.fetchall())

        
        #--RESET GRAPH------------------------------------
        graph_init(cursor, 'tesla5forces')
        print('graph has been initialized')


        print('pausing...give you a chance to quit if errors reported')
        time.sleep(5)

        for query in node_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        for query in edge_queries:
            cursor.execute(f"SELECT * FROM cypher('{GraphDBname}', " + query)
        conn.commit()
    except Exception as ex:
        print(type(ex), ex)
        # if exception occurs, you must rollback all transaction. 
        conn.rollback()

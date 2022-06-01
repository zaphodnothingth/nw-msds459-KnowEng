import psycopg2 
from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader 
spec = spec_from_loader(".creds", SourceFileLoader(".creds", "./.creds"))
creds = module_from_spec(spec)
spec.loader.exec_module(creds)

DATABASE = {
    # "drivername": "postgresql",
    "host": "192.46.216.236",
    "port": "5432",
    "user": "stevensa",
    "password": creds.password,
    "dbname": "stevensa",
}
keepalive_kwargs = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 5,
    "keepalives_count": 5,
}

conn = psycopg2.connect(**DATABASE, **keepalive_kwargs)

def create_node_query(lbl, attr_dict: dict = {}):
    attrs = '{' + ','.join([str(item[0]) + ":'" + str(item[1]) + "'" for item in attr_dict.items()]) + '}'
    cQtmp = f"$$ CREATE (:{lbl} {attrs}"
    cQtmp = cQtmp + f") $$) as (n agtype);"
    return cQtmp


def create_edge_query(lblA,lblB,filterAkey,filterA,filterBkey,filterB,relType):
    cQtmp = f"$$ MATCH (a:{lblA}), (b:{lblB}) WHERE a.{filterAkey}='{filterA}' AND b.{filterBkey}='{filterB}'"
    cQtmp = cQtmp + f" CREATE (a)-[rel:{relType}]->(b) RETURN rel $$) as (e agtype);"
    return cQtmp


def node_set_query(lbl, filterAkey, filterA, attr_dict: dict={}):
    attrs = ','.join(["a." + str(item[0]) + "='" + str(item[1]) + "'" for item in attr_dict.items()])
    cQtmp = f"$$ MERGE (a:{lbl} {{{filterAkey}:'{filterA}'}})"
    cQtmp = cQtmp + f" SET {attrs}"
    cQtmp += " RETURN a $$) as (n agtype);"
    return cQtmp


def graph_init(cursor, GraphDBname, hard=False):
    #Test the connection and cursor by selecting the current_date from the postgreSQL server
    cursor.execute('SELECT CURRENT_DATE;')
    print("If your connection worked the current date is:")
    print(cursor.fetchall())

    if hard: # a hard reset drops & recreates database. use default if graph exists and you don't want to drop all objects
        try:
            cursor.execute(f"SELECT * from ag_catalog.drop_graph('{GraphDBname}', true);")
        except: None
        try:
            cursor.execute(f"SELECT * from ag_catalog.create_graph('{GraphDBname}');")
        except: None

    cursor.execute("LOAD '$libdir/plugins/age';")
    cursor.execute("SET search_path = ag_catalog, '$user', public;")
    print('graph has been initialized')


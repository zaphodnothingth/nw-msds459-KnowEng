from flask import Flask, redirect, url_for, render_template, request
import json
import csv
import psycopg2
import re
import pandas as pd

from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader 
spec = spec_from_loader(".creds", SourceFileLoader(".creds", "./.creds"))
creds = module_from_spec(spec)
spec.loader.exec_module(creds)

app = Flask(__name__)

GraphDBname = 'tesla5forces'
DATABASE = {
    # "drivername": "postgresql",
    "host": "192.46.216.236",
    "port": "5432",
    "user": "stevensa",
    "password": creds.password,
    "dbname": "stevensa",
}

ip = DATABASE['host']
user = DATABASE['user']
pwd = DATABASE['password']
db = DATABASE['dbname']


# Redirect to another url (by def name
@app.route("/")
def redirecthome():
    
    
    return redirect(url_for("home"))

#you can have mutiple url locations use the same def
@app.route("/home/", methods=["GET", "POST"])
@app.route("/home", methods=["GET", "POST"])
def home():
    pageTitle = "Tesla Graph Document Search"
    
    if request.method == "GET":
         return render_template(
            "main.html", 
            title = pageTitle, 
            humanReadableQueryVerify = None,
            res_fields = None,
            res_data = None,
            #Not used, but still in template
            progress_bar_pct = 0, step_running_name = "not used"
            )
    elif request.method == "POST":
        #Someone Has at least clicked the "Magic" button grab the value from the webpage
        #NOTE never trust the end user you should make sure the request is valid and not malicious :)
        webquestion = request.form["nmwebquestion"]

        processedQuestionDictionary = extractIntent('doc', webquestion)
        
        if processedQuestionDictionary == "Error":
            return redirect(url_for("home"))
        
        queryverify = processedQuestionDictionary['humanReadableQueryVerify']
        fields = processedQuestionDictionary['res_fields']
        data = processedQuestionDictionary['res_data']
        
        #return processedQuestionDictionary
        
        #Link to the main.html pages in the templates folder for rendering in the user's web browser
        
        
        return render_template("main.html", title = pageTitle, humanReadableQueryVerify = queryverify, res_fields = fields, res_data = data, progress_bar_pct = 0, step_running_name = "not used")


#you can have mutiple url locations use the same def
@app.route("/relationships/", methods=["GET", "POST"])
@app.route("/relationships", methods=["GET", "POST"])
def relationships():
    pageTitle = "Tesla Graph Relationship Search"
    
    if request.method == "GET":
         return render_template(
            "main.html", 
            title = pageTitle, 
            humanReadableQueryVerify = None,
            res_fields = None,
            res_data = None,
            #Not used, but still in template
            progress_bar_pct = 0, step_running_name = "not used"
            )
    elif request.method == "POST":
        #Someone Has at least clicked the "Magic" button grab the value from the webpage
        #NOTE never trust the end user you should make sure the request is valid and not malicious :)
        webquestion = request.form["nmwebquestion"]

        processedQuestionDictionary = extractIntent('relationship', webquestion)
        
        if processedQuestionDictionary == "Error":
            return redirect(url_for("home"))
        
        queryverify = processedQuestionDictionary['humanReadableQueryVerify']
        fields = processedQuestionDictionary['res_fields']
        data = processedQuestionDictionary['res_data']
        
        #return processedQuestionDictionary
        
        #Link to the main.html pages in the templates folder for rendering in the user's web browser
        
        
        return render_template("main.html", title = pageTitle, humanReadableQueryVerify = queryverify, res_fields = fields, res_data = data, progress_bar_pct = 0, step_running_name = "not used")
      
      
#This is the main code to determine the users intent To check if the webpage is working pass TEST to this
def extractIntent(qtype, UserInput):
    if qtype == 'doc':
        if UserInput == "":
            #You could do some feedback but for now it just goes back home
            return "Error"
        
        if UserInput == "TEST":
            TestDict = {
                'humanReadableQueryVerify': "The Intent of your request is to test Flask Output",
                'res_fields': ['Data Colunm Name 1', '2nd Data Column', 'URL'],
                'res_data': [
                    ["This is the data in the first Column first row", "2nd column first row", 'https://www.google.com'],
                    ["first Column 2nd row", "2nd column 2nd row", 'http://www.yahoo.com'],
                    ["first Column 3nd row", "2nd column 3nd row", 'http://www.bing.com']
                        ]
                }
            return TestDict
        
        if UserInput == "TEST SQL":
            SQL_Results = runSQL("""
                SELECT CURRENT_DATE, random() as Rand_num, table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema not in ('pg_catalog', 'information_schema')
                ORDER BY table_schema,table_name;
                """)
            SQL_Results['humanReadableQueryVerify'] = "Testing PostgreSQL and Database Connection"
            
            return SQL_Results

        if UserInput == "TEST OpenCypher":
            SQL_Results = runCypher(f"""
                SELECT * FROM cypher('{GraphDBname}', $$ MATCH (a) RETURN a.entity, label(a), id(a) $$) as (NodeName agtype, NodeLabel agtype, NodeID agtype);
                """)
            SQL_Results['humanReadableQueryVerify'] = "Testing PostgreSQL and Cypher Query Results"
            
            return SQL_Results


        #DEFAULT TO AN ALL MATCH full text search within Postgres Document DB.
        else: 
            InputWordList = UserInput.split(" ")
            s = " & "
            SearchParmaters = s.join(InputWordList)

            InitSQL_Results = runTextSearch(SearchParmaters)

            SQL_Results = runTextMatchSummary(InitSQL_Results, SearchParmaters)

            SQL_Results['humanReadableQueryVerify'] = f"Searching Documents that contain {SearchParmaters}"

            return SQL_Results
    else:
        InputWordList = UserInput.split(" ")
        s = " & "
        SearchParmaters = s.join(InputWordList)

        SQL_Results = runRelSearch(SearchParmaters)

        SQL_Results['humanReadableQueryVerify'] = f"Searching Documents that contain {SearchParmaters}"

        return SQL_Results


def runSQL(qry):
    conn = psycopg2.connect(host=ip, database=db, user=user, password=pwd)
    cur = conn.cursor()

    #Test the connection and cursor by selecting the current_date from the postgreSQL server   
    cur.execute(qry)

    data_rows = []
    #Return all the column names
    fields = [desc[0] for desc in cur.description]
    for row in cur:
        data_rows.append(row)
    queryResults = {'res_fields': fields,
                    'res_data': data_rows
                    }

    cur.close()
    if conn is not None:
        conn.close()

    return queryResults
 
def runCypher(qry):

 
    conn = psycopg2.connect(host=ip, database=db, user=user, password=pwd)
    cur = conn.cursor()
    
    #Load Prereqs for Cypher
    cur.execute("LOAD '$libdir/plugins/age';")
    cur.execute("SET search_path = ag_catalog, '$user', public;")

    #Test the connection and cursor by selecting the current_date from the postgreSQL server   
    cur.execute(qry)

    data_rows = []
    #Return all the column names
    fields = [desc[0] for desc in cur.description]
    for row in cur:
        data_rows.append(row)
    queryResults = {'res_fields': fields,
                    'res_data': data_rows
                    }


    cur.close()
    if conn is not None:
        conn.close()
    
    return queryResults

def runTextSearch(input):

    qry = f"""
    with results as 
        (
        SELECT url, ts_rank_cd(to_tsvector(entities), query) AS rank
        FROM public.sites2companies, to_tsquery('{input}') query
        WHERE to_tsvector(entities) @@ query
        ORDER BY rank DESC
        )
    select distinct(url) from results
    LIMIT 5;
    """ 
    
    conn = psycopg2.connect(host=ip, database=db, user=user, password=pwd)
    cur = conn.cursor()

    cur.execute(qry)
    data_ids = []
    for row in cur:
        data_ids.append(row[0])

    cur.close()
    if conn is not None:
        conn.close()

    return data_ids


def runRelSearch(input):

    qry = f"""
    SELECT tuples, ts_rank_cd(to_tsvector(tuples), query) AS rank
    FROM public.tuples, to_tsquery('{input}') query
    WHERE to_tsvector(tuples) @@ query
    ORDER BY rank DESC
    LIMIT 5;
    """ 
    
    conn = psycopg2.connect(host=ip, database=db, user=user, password=pwd)
    cur = conn.cursor()

    cur.execute(qry)
    data_rows = []
    #Return all the column names
    fields = [desc[0] for desc in cur.description]
    for row in cur:
        data_rows.append(row)
    queryResults = {'res_fields': fields,
                    'res_data': data_rows
                    }

    cur.close()
    if conn is not None:
        conn.close()

    return queryResults


def runTextMatchSummary(ID_list, SearchParamater):
    print(ID_list)
    str_ID_list = str(ID_list).replace('[','(').replace(']',')')
    print(str_ID_list)
    #idlst = idlst[2:]

    qry = f"""
    SELECT split_part(properties::varchar,'"',4) company, 
    ts_headline('english', body, query, 'MaxFragments=10, MaxWords=7, MinWords=3')
   from (select *, to_tsquery('english', '{SearchParamater}') query 
         from public.sites2companies where url in {str_ID_list}
         ) a
  ;
    """
    print("----------")
    print(qry)
    print("----------")


    conn = psycopg2.connect(host=ip, database=db, user=user, password=pwd)
    cur = conn.cursor()

    cur.execute(qry)

    data_rows = []
    #Return all the column names
    fields = [desc[0] for desc in cur.description]
    for row in cur:
        data_rows.append(row)
    queryResults = {'res_fields': fields,
                    'res_data': data_rows
                    }

    cur.close()
    if conn is not None:
        conn.close()

    return queryResults

if __name__ == "__main__":
    #adding the host 0.0.0.0 any computer on the local network should be albe to open the webpage
    app.run(host='0.0.0.0', debug=True, port = 8080)

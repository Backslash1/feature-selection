# -*- coding: utf-8 -*-
import datetime
import argparse
import os
import json
from sklearn import pipeline
import yaml
import json, requests, textwrap
import subprocess
import sys
from requests_negotiate import HTTPNegotiateAuth
import time
import pandas as pd
import numpy as np
import plotly
from sqlalchemy import create_engine
import redis
import shutil
import plotly.express as px
import chart_studio.plotly as py
import plotly.graph_objs as go
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pdpipe as pdp
from pyArango.connection import Connection
from elasticsearch import Elasticsearch
from queryRecommender.recommender import recommend, currentQuery
from util import knox_server
from util.knox_server import knox_data_transfer
from fastapi import FastAPI, Request, UploadFile, File, Body
import uvicorn
# from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse ,RedirectResponse
from typing import List, Dict, Optional
from warnings import filterwarnings
filterwarnings("ignore")
from util.preprocess_utils import *
from conf.JBDL_Conf import livy_session
import pymongo

env = os.environ['ENVTYPE']
host = os.environ['Address_IP']
port = os.environ['port2900']
dsp_config_file = 'conf/' + env + '/dsp_config.yml'
user_name = os.environ['SHINYPROXY_USERNAME']
firstname = user_name.split(".")[0]
lastname = user_name.split(".")[1]
first = firstname[0].upper() + firstname[1:]
last = lastname[0].upper() + lastname[1:]
user_name = first + "." + last

with open(dsp_config_file, 'r',encoding='utf8') as stream:
    server_config = yaml.safe_load(stream)
    livy_config = server_config['server']


    host= server_config['server']['redis_host']
    redisport = server_config['server']['redis_port']

if(env == 'async default'):
    connection_url =  'postgresql://jio:12345@' + host + ':5432/automl'
else:
    connection_url = 'postgresql://jio:qwerty@curiopostgres:5432/automl'

print("Postgres Connection URL : ", connection_url, file=sys.stderr)

engine = create_engine(connection_url)


# app = Flask(__name__)
# sslify = SSLify(app)
# app.config["DEBUG"] = False
app = FastAPI()
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

UPLOAD_FOLDER='static'

# app.config['UPLOAD_PATH'] = '/workspace/DataCurio/data'

# cors = CORS(app, resources={r"/*": {"origins": "*"}})

dict_session_data = {}
from util import knox_server, user_activity_db

async def add_to_db(dictionary_data):
    id_ss = str(list(dictionary_data.keys())[0])
    t = pd.DataFrame(dictionary_data[id_ss].items(), columns = list(dictionary_data[id_ss].keys()))
    table_name = list(dictionary_data.keys())[0]

    t.to_sql(table_name, engine, schema = 'logs', if_exists='replace', index = False)

    engine.execute("INSERT INTO logs." + table_name + " VALUES (%s, %s, %s)", tuple(dictionary_data[id_ss].values()))


# @app.api_route('/api/v1/Upload_File',methods=['POST'])
# async def Upload_File(request: Request):
#     reqJSON = await request.json()
#     file_uploaded = reqJSON['upload_file']
# #     request.get_json(force=True)
#     session_id = request.json['session_id']
#     username = json.loads(json.loads(read_cache(str(session_id)))['user_name'])
#     password = json.loads(json.loads(read_cache(str(session_id)))['pass_code'])

#     print(file_uploaded.filename)

#     filename = secure_filename(file_uploaded.filename)
#     input_save_path = os.path.join(UPLOAD_FOLDER, filename)
#     file_uploaded.save(input_save_path)


#     output = knox_object.upload_to_hadoop(username,password,input_save_path)
#     print(output,file=sys.stderr)

#     return {'result': output}


# @app.api_route("/api/v1/login_hadoop",methods=['POST'])
@app.api_route('/api/v1/login_hadoop',methods=['POST'])
async def hadoop_login(request: Request):
    reqJSON = await request.json()
    username = reqJSON['username']
    password = reqJSON['password']

    print("hadoop login => username : ", username,file=sys.stderr)
    time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")
    print(time_stamp)
    try:
        json_previous_results = json.loads(read_cache(str(username)))
        if(server_obj.checkSessionState(str(json_previous_results['session_id']))):
            user_activity_db.write_log({'session_id':json_previous_results['session_id'], "user_name":str(username),'time_stamp':time_stamp,'activity':'login'})
            return {'session_id':json_previous_results['session_id']}
        else:
            raise Exception
    except:
        session_id,session_url,url = server_obj.start_session(username,'sessions')
        result_output = server_obj.show_databases(session_id)
        print(session_id)

        result_json = json.loads(result_output)
#         print(result_json,file=sys.stderr)
        write_cache(str(session_id),{'user_name':username ,'pass_code':password})
        write_cache(str(username),{'session_id': session_id, 'databases':result_json['database_detail']})

        user_activity_db.log({'session_id':str(session_id),"user_name":str(username), 'time_stamp':time_stamp,'operation_type':'login','operation':'signing'})
        #print(result_json['database_detail'],file=sys.stderr)

        return {'session_id': session_id}

@app.api_route('/api/v1/send_database',methods=['POST'])
async def send_databse(request: Request):
    reqJSON = await request.json()
    print(reqJSON)
    session_id = reqJSON['session_id']
    user_data = json.loads(read_cache(str(session_id)))
    json_previous_results = json.loads(read_cache(user_data['user_name']))
    return json_previous_results

# @app.api_route("/api/v1/terminal_command/<session_id>/<command>",methods=['POST'])
# async def terminal_command(request: Request,session_id,command):
# #     session_id = request.json['session_id']
# #     command = request.json['command']

#     user_data = json.loads(read_cache(str(session_id)))

#     result_output = server_obj.terminal_command(session_id,command)
#     result_json = json.loads(result_output)
#     return result_json



@app.api_route("/api/v1/show_table",methods=['POST'])
async def show_table(request: Request):
    reqJSON = await request.json()
    print(reqJSON)
    database_name = reqJSON['database_name']
    table_name = reqJSON['table_name']
    session_id = reqJSON['session_id']
    start_row = reqJSON['start_row']
    time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")


    start_row = 0
    user_data = json.loads(read_cache(str(session_id)))

    try:
        previous_results = json.loads(read_cache('{}.{}.{}'.format(database_name,database_name,str(start_row))))

        user_activity_db.log({'session_id':str(session_id),"user_name":str(user_data['user_name']),         'time_stamp':time_stamp,'operation_type':'show_table','operation':'{}.{}'.format(database_name,table_name)})

        result_json = previous_results


    except:
        result_output = server_obj.show_tabledata(session_id,database_name,table_name)

        result_json = json.loads(result_output)

        write_cache('{}.{}.{}'.format(database_name,table_name,str(start_row)),result_json)

        user_activity_db.log({'session_id':str(session_id),"user_name":str(user_data['user_name']),         'time_stamp':time_stamp,'operation_type':'show_table','operation':'{}.{}'.format(database_name,table_name)})


    if  len(result_json['table_detail']) == 0:
#         except:
        return [{'error':"Permission Denied"}]
    else:

        return json.loads(result_json['table_detail'][str(database_name)+'.'+str(table_name)])



# @app.api_route("/api/v1/kill_session",methods=['POST'])
# async def hadoop_logout(request: Request):
#     reqJSON = await request.json()
#     print(reqJSON)
#     print(reqJSON['session_id'])
#     session_id = reqJSON['session_id']
#     time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")

#     user_data = json.loads(read_cache(str(session_id)))
#     user_activity_db.log({'session_id':str(session_id),"user_name":str(user_data['user_name']),         'time_stamp':time_stamp,'operation_type':'logout','operation':'signing'})
#     dummy = server_obj.delete_session(session_id)

#     time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")
# #     add_to_db({str(session_id):{"username":str(username),'timestamp':time_stamp,'activity':'logout'}})

#     return {'success': dummy}

@app.api_route("/api/v1/test_report", methods=['GET'])
async def test_report(request: Request):
    return templates.TemplateResponse("test.html")


# @app.api_route("/api/v1/report_database",methods=['POST'])
# async def report_database(request: Request):
#     reqJSON = await request.json()
#     username = reqJSON['username']
#     password = reqJSON['password']

#     print("report database => username :", username,file=sys.stderr)
#     time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")

#     try :
#         json_previous_results = json.loads(read_cache(str(username) + '_profiling_tree'))
#         return json_previous_results
#     except :

#         json_previous_results = json.loads(read_cache(str(username)))
#         session_id = json_previous_results['session_id']
#         #session_id,session_url,url = server_obj.start_session(username, 'sessions')
#         paths_to_look = "/data/network/lsr/dataprofilereports/ /data/network/lsmr/dataprofilereports/  /data/network/probes/dataprofilereports/  /data/devices/tac/dataprofilereports/  /data/devices/fttx/dataprofilereports/ "
#         command = "hdfs dfs -ls -R " + paths_to_look + " | grep .html | awk '{print $8}' "

#         result_output = server_obj.terminal_command(session_id, command)

#         path_list = []
#         result = result_output.splitlines()

#         for i in range(len(result)):
#             if result[i].startswith('/data/') and 'dataprofilereports' in result[i]:
#                 path_list.append(result[i])
#         print("report database => path_list : ", path_list, file=sys.stderr)
#         print("\n", file=sys.stderr)

#         jsondata = {}

#         for path in path_list:

#             items = path.split("/")

#             base = items[1]
#             basedatasource = items[2]
#             datasource = items[3]
#             dataprofilereports = items[4]
#             tablename = items[5]
#             partitiondate = items[6]
#             reportname = items[7]
#             reportsplit = reportname.split('_')
#             uuid = reportsplit[len(reportsplit)-1].split(".")[0]

#             try:
#                 tree = jsondata[base]
#                 try:
#                     tree = jsondata[base][basedatasource]
#                     try:
#                         tree = jsondata[base][basedatasource][datasource]
#                         try:
#                             tree = jsondata[base][basedatasource][datasource]
#                             try:
#                                 tree = jsondata[base][basedatasource][datasource][tablename]
#                                 try:
#                                     tree = jsondata[base][basedatasource][datasource][tablename][partitiondate]
#                                     try:
#                                         tree = jsondata[base][basedatasource][datasource][tablename][partitiondate][uuid]
#                                     except:
#                                         jsondata[base][basedatasource][datasource][tablename][partitiondate][uuid] =  uuid
#                                 except:
#                                     jsondata[base][basedatasource][datasource][tablename][partitiondate] = {uuid : uuid}
#                             except:
#                                 jsondata[base][basedatasource][datasource][tablename] =  {partitiondate: {uuid : uuid }}
#                         except:
#                             jsondata[base][basedatasource][datasource] = {tablename: {partitiondate: {uuid : uuid}}}
#                     except:
#                         jsondata[base][basedatasource][datasource] =  {tablename: {partitiondate: {uuid : uuid}}}
#                 except:
#                     jsondata[base][basedatasource] =  {datasource: {tablename: {partitiondate: { uuid : uuid}}}}
#             except:
#                 jsondata[base] = {basedatasource: {datasource: {tablename: {partitiondate: { uuid : uuid}}}}}

#         json_obj = json.dumps(jsondata)

#         print("report database => jsno obj : ", json_obj, file=sys.stderr)
#         print("\n", file=sys.stderr)

#         write_cache(str(username) + '_profiling_tree', json_obj)
#         return json_obj

# @app.api_route("/api/v1/refresh_report_database",methods=['POST'])
# async def refresh_report_database(request: Request):
#     reqJSON = await request.json()
#     username = reqJSON['username']
#     password = reqJSON['password']

#     print("report database => username :", username,file=sys.stderr)
#     time_stamp = datetime.datetime.now().strftime("%d-%m-%Y_%I-%M-%S_%f_%p")

#     try :
#         json_previous_results = json.loads(read_cache(str(username)))
#         session_id = json_previous_results['session_id']
#     except :
#         session_id,session_url,url = server_obj.start_session(username, 'sessions')

#     try :
#         paths_to_look = "/data/network/lsr/dataprofilereports/ /data/network/lsmr/dataprofilereports/  /data/network/probes/dataprofilereports/  /data/devices/tac/dataprofilereports/  /data/devices/fttx/dataprofilereports/ "
#         command = "hdfs dfs -ls -R " + paths_to_look + " | grep .html | awk '{print $8}' "

#         result_output = server_obj.terminal_command(session_id, command)

#         path_list = []
#         result = result_output.splitlines()

#         for i in range(len(result)):
#             if result[i].startswith('/data/') and 'dataprofilereports' in result[i]:
#                 path_list.append(result[i])
#         print("report database => path_list : ", path_list, file=sys.stderr)
#         print("\n", file=sys.stderr)

#         jsondata = {}

#         for path in path_list:

#             items = path.split("/")

#             base = items[1]
#             basedatasource = items[2]
#             datasource = items[3]
#             dataprofilereports = items[4]
#             tablename = items[5]
#             partitiondate = items[6]
#             reportname = items[7]
#             reportsplit = reportname.split('_')
#             uuid = reportsplit[len(reportsplit)-1].split(".")[0]

#             try:
#                 tree = jsondata[base]
#                 try:
#                     tree = jsondata[base][basedatasource]
#                     try:
#                         tree = jsondata[base][basedatasource][datasource]
#                         try:
#                             tree = jsondata[base][basedatasource][datasource]
#                             try:
#                                 tree = jsondata[base][basedatasource][datasource][tablename]
#                                 try:
#                                     tree = jsondata[base][basedatasource][datasource][tablename][partitiondate]
#                                     try:
#                                         tree = jsondata[base][basedatasource][datasource][tablename][partitiondate][uuid]
#                                     except:
#                                         jsondata[base][basedatasource][datasource][tablename][partitiondate][uuid] =  uuid
#                                 except:
#                                     jsondata[base][basedatasource][datasource][tablename][partitiondate] = {uuid : uuid}
#                             except:
#                                 jsondata[base][basedatasource][datasource][tablename] =  {partitiondate: {uuid : uuid }}
#                         except:
#                             jsondata[base][basedatasource][datasource] = {tablename: {partitiondate: {uuid : uuid}}}
#                     except:
#                         jsondata[base][basedatasource][datasource] =  {tablename: {partitiondate: {uuid : uuid}}}
#                 except:
#                     jsondata[base][basedatasource] =  {datasource: {tablename: {partitiondate: { uuid : uuid}}}}
#             except:
#                 jsondata[base] = {basedatasource: {datasource: {tablename: {partitiondate: { uuid : uuid}}}}}

#         json_obj = json.dumps(jsondata)

#         print("report database => jsno obj : ", json_obj, file=sys.stderr)
#         print("\n", file=sys.stderr)

#         write_cache(str(username) + '_profiling_tree', json_obj)
#         return json_obj
#     except :
#         print("Error in refreshing", file=sys.stderr)
#         return {"Result": "Error in refreshing"}


# @app.api_route("/api/v1/show_report",methods=['POST'])
# async def show_report(request: Request):
#     #username = request.json['username']
#     #password = request.json['password']
#     #hdfspath = request.json['hdfspath']

#     username = request.args.get('username')
#     password = request.args.get('password')
#     hdfspath = request.args.get('hdfspath')
#     print("show report => username : ", username, file=sys.stderr)
#     print("show report => hdfspath : ", hdfspath, file=sys.stderr)

#     list_ = hdfspath.split("/")

#     base = list_[1]
#     basedatasource = list_[2]
#     datasource = list_[3]
#     tablename = list_[4]
#     partitiondate = list_[5]
#     uuid = list_[6]

#     reportname = tablename + "_report_" + uuid + ".html"

#     hdfspath = "/" + base + "/" + basedatasource + "/" + datasource + "/dataprofilereports/" + tablename + "/" + partitiondate + "/" + reportname

#     print("show report => hdfspath formatted : ", hdfspath, file=sys.stderr)

#     knox_object = knox_data_transfer("conf/" + env + "/dsp_config.yml","templates/","templates/")

#     try:
#         try:
#             output = knox_object.download_from_hadoop(username, password, hdfspath)
#         except:
#             return {"result": "output error"}
#         try:
#             filename = os.path.basename(hdfspath)
#             print("show report => filename : ", filename, file=sys.stderr)
#             subprocess.getstatusoutput('mv templates/' + filename + ' templates/report.html') # all the downloaded files will be of this name
#         except:
#             return {'result' : 'Error in renaming' }

#         return templates.TemplateResponse("report.html")

#     except:
#         return {'result':"Permission Denied"}

# @app.api_route("/api/v1/check_password",methods=['POST'])
# async def check_password(request: Request):
#     username = request.args.get('username')
#     password = request.args.get('password')
#     hdfspath = 'sample.txt'
#     print("show report => username : ", username, file=sys.stderr)
#     print("show report => hdfspath : ", hdfspath, file=sys.stderr)

#     knox_object = knox_data_transfer("conf/" + env + "/dsp_config.yml","templates/","templates/")

#     try:
#         output = knox_object.upload_to_hadoop(username, password, hdfspath, "/user/" + username+ "/")
#         return {"result": "password correct"}
#     except:
#         return {"result": "password incorrect"}



def write_cache(table_key, dat):
    r = redis.StrictRedis(host=host, port=redisport, db=0, password='redispass')
    redis_key = table_key
    data = json.dumps(dat) #You have to serialize the data to json!
    r.setex(
        redis_key,
        60*60*11, # time to store data
        data
    )


def read_cache(table_key):
    r = redis.StrictRedis(host=host, port=redisport, db=0, password='redispass')
    return r.get(table_key)

@app.api_route('/curator',methods=['GET'], response_class=HTMLResponse)
async def data_curator(request: Request):
    return templates.TemplateResponse('data_curator.html', {"request": request, "host": host, "port": port})

@app.api_route('/eda',methods=['GET'], response_class=HTMLResponse)
async def eda(request: Request):
    return templates.TemplateResponse('eda_python.html', {"request": request, "host": host, "port": port})

@app.api_route('/eda_python',methods=['GET'], response_class=HTMLResponse)
async def eda_python(request: Request):
    return templates.TemplateResponse('eda_python.html', {"request": request, "host": host, "port": port})

# @app.api_route('/eda_pyspark',methods=['GET'], response_class=HTMLResponse)
# async def eda_pyspark(request: Request):
#     return templates.TemplateResponse('eda_pyspark.html', {"request": request, "host": host, "port": port})

@app.api_route('/eda_pyspark',methods=['GET'], response_class=HTMLResponse)
async def eda_pyspark_left_pane(request: Request):
    return templates.TemplateResponse('eda_pyspark_new.html', {"request": request, "host": host, "port": port})

@app.api_route('/workspace', methods=['GET'], response_class=HTMLResponse)
async def user_workspace(request: Request):
    return templates.TemplateResponse('user_workspace.html', {"request": request, "host": host, "port": port})

@app.api_route('/model_workspace', methods=['GET'], response_class=HTMLResponse)
async def model_workspace(request: Request):
    return templates.TemplateResponse('model_workspace.html', {"request": request, "host": host, "port": port})

# @app.api_route('/clustering',methods=['POST'])
@app.api_route('/clustering',methods=['GET'], response_class=HTMLResponse)
async def clustering(request: Request):
    return templates.TemplateResponse('clustering_python.html', {"request": request, "host": host, "port": port})

@app.api_route('/clustering_pyspark',methods=['GET'], response_class=HTMLResponse)
async def clustering_pyspark(request: Request):
    return templates.TemplateResponse('clustering_pyspark.html', {"request": request, "host": host, "port": port})

@app.api_route('/clustering_python',methods=['GET'], response_class=HTMLResponse)
async def clustering_python(request: Request):
    return templates.TemplateResponse('clustering_python.html', {"request": request, "host": host, "port": port}) 

@app.api_route('/featureEngineering',methods=['GET'], response_class=HTMLResponse)
async def featureEngineering(request: Request):
    return templates.TemplateResponse('featureEngineering.html', {"request": request, "host": host, "port": port})

@app.api_route('/featureEngineering_pyspark',methods=['GET'], response_class=HTMLResponse)
async def featureEngineering_pyspark(request: Request):
    return templates.TemplateResponse('featureEngineering_pyspark.html', {"request": request, "host": host, "port": port})

@app.api_route('/goLive',methods=['GET'], response_class=HTMLResponse)
async def goLive(request: Request):
    return templates.TemplateResponse('goLive.html', {"request": request, "host": host, "port": port})

@app.api_route("/modeling",methods=["GET"], response_class=HTMLResponse)
async def modeling(request: Request):
    return templates.TemplateResponse('modeling_python.html', {"request": request, "host": host, "port": port})

@app.api_route('/modeling_python',methods=['GET'], response_class=HTMLResponse)
async def modeling_python(request: Request):
    return templates.TemplateResponse('modeling_python.html', {"request": request, "host": host, "port": port})

@app.api_route('/modeling_pyspark',methods=['GET'], response_class=HTMLResponse)
async def modeling_pyspark(request: Request):
    return templates.TemplateResponse('modeling_pyspark.html', {"request": request, "host": host, "port": port})

# my function
@app.api_route('/featureSelection_python',methods=['GET'], response_class=HTMLResponse)
async def monitor(request: Request):
    return templates.TemplateResponse('feature_selection_python.html', {"request": request, "host": host, "port": port})

@app.api_route('/monitor',methods=['GET'], response_class=HTMLResponse)
async def monitor(request: Request):
    return templates.TemplateResponse('monitor.html', {"request": request, "host": host, "port": port})

@app.api_route('/browse',methods=['GET'], response_class=HTMLResponse)
async def browse(request: Request):
    return templates.TemplateResponse('DataCurioFromHadoop.html',{"request": request, "host": host, "port": port, "user":user_name})

@app.api_route('/navbar',methods=['GET'], response_class=HTMLResponse)
async def navbar(request: Request):
    return templates.TemplateResponse('navbar.html', {"request": request})

@app.api_route('/mlflow',methods=['GET'])
async def mlflow(request: Request):
    port5000 = os.environ['port5000']
    return {"url": "http://" + host + ":" + port5000}

@app.api_route('/notebook_explorer',methods=['GET'], response_class=HTMLResponse)
async def notebook_explorer(request: Request):
    if (env == "qa"):
        return RedirectResponse('https://qajdsp1.jio.com:31192/app/jdspv2')
    else:
        return RedirectResponse('https://jdsp1.jio.com:31192/app/ne_coe')

@app.api_route("/recommender", methods=['POST'])
async def recommender(request: Request):
    reqJSON = await request.json()
    query = reqJSON['query'].lower()
    meta = reqJSON['meta']
    print("query : ", query, file=sys.stderr)
    print("meta : ", meta, file = sys.stderr)

    nextTop5 = recommend(query)
    current_query = currentQuery(query, meta)
    return {"currentQuery": current_query, "nextTop5" : nextTop5}

@app.api_route("/auto_complete", methods=["POST"])
async def filesearch_autocomplete_pyspark(data: str = Body(..., embed=True)):
    data = data.lower()
    es = Elasticsearch(["10.159.18.75:9200"])
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "filepath": {
                                "value": "*" + str(data) + "*"
                            }
                        }
                    }
                ]
            }
        }
    }
    
    resp = es.search(body=query)
    file_list = []
    if(resp['hits']['total']['value']):
        files = resp['hits']
        files = files['hits']
        for f in files:
            file_list.append(f['_source']['filepath'])
    
    return json.dumps(file_list)

@app.api_route("/api/v1/ListOfDF_hdfs", methods=['POST'])
async def dataframe_list_hdfs(session_id: int = Body(..., embed = True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')

    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
        'code': textwrap.dedent("""
        from pyspark.sql import DataFrame
        import json
        df_list = [k for (k, v) in globals().items() if isinstance(v, DataFrame)]
        print(json.dumps(df_list))
        """)
        }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers, auth=auth)

    r = requests.get(statements_url, headers=headers, auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers, auth=auth)

    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers, auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']

    return {json.dumps(output)}

@app.api_route("/api/v1/eda/data_profiling", methods=['POST'])
async def data_profiling(columns: List[str],filepath: str = Body(..., embed = True)):
    df = pd.read_csv(filepath, usecols= columns)
    dprof_df = pd.DataFrame({'columns_names': df.columns,'data_types': [str(x) for x in df.dtypes]})
    
    num_rows = len(df)
    dprof_df['num_rows'] = num_rows
    
    df_nacounts = df.isnull().sum()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['columns_names', 'num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['columns_names'], how = 'left')
    
    number_spaces = []
    for column_name in df.columns :
        if df[column_name].dtype is not object:
            number_spaces.append(0)
        else:
            number_spaces.append(df[column_name].str.isspace().sum())        
    dprof_df['num_spaces'] = number_spaces
    
    num_blank = [df[column_name].where(df[column_name]=='').sum() for column_name in df.columns]
    dprof_df['num_blank'] = num_blank
    
    desc_df = df.describe().transpose()[['count', 'mean', 'std', 'min', 'max']]
    desc_df.columns = ['count', 'mean', 'std', 'min', 'max']
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'columns_names'
    desc_df = desc_df[['columns_names','count', 'mean', 'std']]
    dprof_df = pd.merge(dprof_df, desc_df , on = ['columns_names'], how = 'left')
    
    allminvalues = []
    allmaxvalues = []
    allmincounts = []
    allmaxcounts = []
    for column in df.columns:
        if df[column].dtype != 'object':
            min_value = df[column].min(skipna = True)
            max_value = df[column].max(skipna = True)
            
            allminvalues.append(min_value)
            allmaxvalues.append(max_value)
            allmincounts.append(df[column].where(df[column]==min_value).count())
            allmaxcounts.append(df[column].where(df[column]==max_value).count())
        else:
            allminvalues.append('NA')
            allmaxvalues.append('NA')
            allmincounts.append('NA')
            allmaxcounts.append('NA')

    
    df_counts = dprof_df[['columns_names']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    df_counts.insert(loc=0, column='counts_min', value=allmincounts)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)
    df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)
    df_counts = df_counts[['columns_names','min','counts_min','max','counts_max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['columns_names'], how = 'left')
    
    unique_count = []
    most_freq_value = []
    most_freq_value_count = []
    least_freq_value = []
    least_freq_value_count = []
    
    for column in df.columns:
        unique_count.append(df[column].nunique())
        most_freq_value.append(df[column].value_counts().reset_index().to_numpy()[0,0]) 
        most_freq_value_count.append(df[column].value_counts().reset_index().to_numpy()[0,1])
        least_freq_value.append(df[column].value_counts().reset_index().to_numpy()[-1,0])
        least_freq_value_count.append(df[column].value_counts().reset_index().to_numpy()[-1,1])
        
    dprof_df['num_distinct'] = unique_count
    dprof_df['most_freq_value'] = most_freq_value
    dprof_df['most_freq_value_count'] = most_freq_value_count
    dprof_df['least_freq_value'] = least_freq_value
    dprof_df['least_freq_value_count'] = least_freq_value_count
    dprof_df = dprof_df.round(decimals = 2)

    output = dprof_df.to_json(orient="records")
    output = json.dumps(output)

    return {"data_profile": output}

@app.api_route("/api/v1/eda/data_profiling_hdfs", methods=['POST'])
async def data_profiling_hdfs(columns: List[str], session_id: str = Body(..., embed = True), filepath: str = Body(..., embed = True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'

    data = {'code': textwrap.dedent("""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.functions import isnan, when, count, col
    import os
    df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
    globals()[df_filename] = spark.read.csv( '""" + filepath + """', inferSchema=True, header=True)
    data_all_df = globals()[df_filename]
    
    columns2Bprofiled = """ + str(columns) + """
    data_df = data_all_df.select(columns2Bprofiled)
    global schema_name, table_name
    if not 'schema_name' in globals():
        schema_name = 'schema_name'
    if not 'table_name' in globals():
        table_name = 'table_name'
    dprof_df = pd.DataFrame({'schema_name':[schema_name] * len(data_df.columns),'table_name':[table_name] * len(data_df.columns), 'columns_names':data_df.columns,'data_types':[x[1] for x in data_df.dtypes]})
    dprof_df = dprof_df[['schema_name','table_name','columns_names', 'data_types']]
    
    num_rows = data_df.count()
    dprof_df['num_rows'] = num_rows
    
    df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['columns_names','num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['columns_names'], how = 'left')
    
    num_spaces = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
    dprof_df['num_spaces'] = num_spaces
    num_blank = [data_df.where(F.col(c)=='').count() for c in data_df.columns]
    dprof_df['num_blank'] = num_blank
    
    desc_df = data_df.describe().toPandas().transpose()
    desc_df.columns = ['count', 'mean', 'stddev', 'min', 'max']
    desc_df = desc_df.iloc[1:,:]
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'columns_names'
    desc_df = desc_df[['columns_names','count', 'mean', 'stddev']]
    dprof_df = pd.merge(dprof_df, desc_df , on = ['columns_names'], how = 'left')
    
    allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmincounts = [data_df.where(col(x) == int(y)).count() for x,y in zip(columns2Bprofiled, allminvalues)]
    allmaxcounts = [data_df.where(col(x) == int(y)).count() for x,y in zip(columns2Bprofiled, allmaxvalues)]
    
    df_counts = dprof_df[['columns_names']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    df_counts.insert(loc=0, column='counts_min', value=allmincounts)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)
    df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)
    df_counts = df_counts[['columns_names','min','counts_min','max','counts_max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['columns_names'], how = 'left')
    
    dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
    dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=False).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)
    dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=True).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)
    dprof_df = dprof_df.round(decimals = 2)
    result = dprof_df.to_json(orient="records")
    print(json.dumps(result))
    del[[data_all_df]]
    del[[data_df]]
    """)
    }
    
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)
    
    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']
    
    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        
    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']

    return {"data_profile": output}

@app.api_route("/api/v1/eda/df_data_profiling_hdfs", methods=['POST'])
async def df_data_profiling_hdfs(columns: List[str], session_id: str = Body(..., embed = True), dataframe: str = Body(..., embed = True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'

    data = {'code': textwrap.dedent("""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.functions import isnan, when, count, col
    data_all_df = """ + str(dataframe) + """
    columns2Bprofiled = """ + str(columns) + """
    data_df = data_all_df.select(columns2Bprofiled)
    global schema_name, table_name
    if not 'schema_name' in globals():
        schema_name = 'schema_name'
    if not 'table_name' in globals():
        table_name = 'table_name'
    dprof_df = pd.DataFrame({'schema_name':[schema_name] * len(data_df.columns),'table_name':[table_name] * len(data_df.columns), 'columns_names':data_df.columns,'data_types':[x[1] for x in data_df.dtypes]})
    dprof_df = dprof_df[['schema_name','table_name','columns_names', 'data_types']]
    
    num_rows = data_df.count()
    dprof_df['num_rows'] = num_rows
    
    df_nacounts = data_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in data_df.columns if data_df.select(c).dtypes[0][1]!='timestamp']).toPandas().transpose()
    df_nacounts = df_nacounts.reset_index()
    df_nacounts.columns = ['columns_names','num_null']
    dprof_df = pd.merge(dprof_df, df_nacounts, on = ['columns_names'], how = 'left')
    
    num_spaces = [data_df.where(F.col(c).rlike('^\\s+$')).count() for c in data_df.columns]
    dprof_df['num_spaces'] = num_spaces
    num_blank = [data_df.where(F.col(c)=='').count() for c in data_df.columns]
    dprof_df['num_blank'] = num_blank
    
    desc_df = data_df.describe().toPandas().transpose()
    desc_df.columns = ['count', 'mean', 'stddev', 'min', 'max']
    desc_df = desc_df.iloc[1:,:]
    desc_df = desc_df.reset_index()
    desc_df.columns.values[0] = 'columns_names'
    desc_df = desc_df[['columns_names','count', 'mean', 'stddev']]
    dprof_df = pd.merge(dprof_df, desc_df , on = ['columns_names'], how = 'left')
    
    allminvalues = [data_df.select(F.min(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmaxvalues = [data_df.select(F.max(x)).limit(1).toPandas().iloc[0][0] for x in columns2Bprofiled]
    allmincounts = [data_df.where(col(x) == int(y)).count() for x,y in zip(columns2Bprofiled, allminvalues)]
    allmaxcounts = [data_df.where(col(x) == int(y)).count() for x,y in zip(columns2Bprofiled, allmaxvalues)]
    
    df_counts = dprof_df[['columns_names']]
    df_counts.insert(loc=0, column='min', value=allminvalues)
    df_counts.insert(loc=0, column='counts_min', value=allmincounts)
    df_counts.insert(loc=0, column='max', value=allmaxvalues)
    df_counts.insert(loc=0, column='counts_max', value=allmaxcounts)
    df_counts = df_counts[['columns_names','min','counts_min','max','counts_max']]
    dprof_df = pd.merge(dprof_df, df_counts , on = ['columns_names'], how = 'left')
    
    dprof_df['num_distinct'] = [data_df.select(x).distinct().count() for x in columns2Bprofiled]
    dprof_df['most_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=False).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['most_freq_value'] = [x[0] for x in dprof_df['most_freq_valwcount']]
    dprof_df['most_freq_value_count'] = [x[1] for x in dprof_df['most_freq_valwcount']]
    dprof_df = dprof_df.drop(['most_freq_valwcount'],axis=1)
    dprof_df['least_freq_valwcount'] = [data_df.groupBy(x).count().sort("count",ascending=True).limit(1).toPandas().iloc[0].values.tolist() for x in columns2Bprofiled]
    dprof_df['least_freq_value'] = [x[0] for x in dprof_df['least_freq_valwcount']]
    dprof_df['least_freq_value_count'] = [x[1] for x in dprof_df['least_freq_valwcount']]
    dprof_df = dprof_df.drop(['least_freq_valwcount'],axis=1)
    dprof_df = dprof_df.round(decimals = 2)
    result = dprof_df.to_json(orient="records")
    print(json.dumps(result))
    del[[data_all_df]]
    del[[data_df]]
    """)
    }
    
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)
    
    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']
    
    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        
    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']

    return {"data_profile": output}

@app.api_route("/api/v1/data_curator/query_generator", methods=['POST'])
async def sql_query_generator(tables: List[Dict], join_on: Optional[List[List[str]]] = Body(None, embed=True), join_type: Optional[str] = Body(None, embed=True), columns_select: Optional[List[str]] = Body(None, embed=True)):
    def column_operations(col_and_op):
        if col_and_op[1] == "sum":
            return("SUM({}) As sum_{}".format(col_and_op[0],col_and_op[0]))
        if col_and_op[1] == "counts":
            return("COUNTS({}) As count_{}".format(col_and_op[0],col_and_op[0]))
        if col_and_op[1] == "":
            if(tables[0]["groupby_col"][0] != col_and_op[0]):
                tables[0]["groupby_col"].append(col_and_op[0])
            else:
                pass
            return("{}".format(col_and_op[0]))
        if col_and_op[1] == "avg":
            return("AVG({}) As avg_{}".format(col_and_op[0],col_and_op[0]))
    def filtering(ops):
        try:
            ops[2] = int(ops[2])
            if type(ops[2]) == int:
                if ops[1]=="equals_to":
                    return("{}={}".format(ops[0],ops[2]))
                if ops[1]=="less_than":
                    return("{}<{}".format(ops[0],ops[2]))
                if ops[1]=="greater_than":
                    return("{}>{}".format(ops[0],ops[2]))
                if ops[1]=="less_than_equals_to":
                    return("{}<={}".format(ops[0],ops[2]))
                if ops[1]=="greater_than_equals_to":
                    return("{}>={}".format(ops[0],ops[2]))
        except Exception as e:
            if type(ops[2]) == str:
                if ops[1]=="equals_to":
                    return('{}="{}"'.format(ops[0],ops[2]))
                else:
                    return(" ") # for demo purpose -> need to handle errors cases
    def sqlQueryGeneratorGroupby(table):
        query = ""
        group_ops = ""
        m = 0
        for col_and_op in table['columns_and_operation']:
            group_ops = group_ops  + column_operations(col_and_op) 
        
            if m < len(table['columns_and_operation'])-1:
                group_ops = group_ops  + ","
            m +=1
        table_name = table['table_address']
        groupby_columns = ""
        j = 0
        for cols in table['groupby_col']:
            if (cols == "none"):
                pass
            elif j==0:
                groupby_columns += cols
                j += 1
            else:
                groupby_columns = groupby_columns + "," + cols
                j += 1
        if table['filter'] != None:
            filters = ""
            k=0
            for ops in table['filter']:
                filters += filtering(ops)
                if k < len(table['filter'])-1:
                    filters += " AND "
                k += 1
        if len(table['groupby_col']) != 0:
            query = "( SELECT " + group_ops + " FROM " + table_name + " WHERE " + filters + " GROUP BY " + groupby_columns + " )"
        else:
            query = "( SELECT " + group_ops + " FROM " + table_name + " WHERE " + filters + " )"
        return query
    def sqlQueryGeneratorJoin(tables, join_on, join_type, columns_select):
        query = ""
        sub_queries = []
        for table in tables:
            sub_queries.append(sqlQueryGeneratorGroupby(table))

        query = "SELECT"
        if columns_select == None:
            query = query + " * FROM "
        else:
            pass
        l = 0
        for sub_query in sub_queries:
            if l < len(sub_queries) and l!=0:
                query = query  + join_type + " " + sub_query +  " table{} ".format(l)
            else :
                query = query + sub_query +  " table{} ".format(l)
            l += 1
        query = query + "ON" + " table0.{} ".format(join_on[0][1])+"="+" table1.{} ".format(join_on[1][1])
        return query
    
    query = ""
    if(join_on == None and join_type == None):
        query = sqlQueryGeneratorGroupby(tables[0])
    else:
        query = sqlQueryGeneratorJoin(tables, join_on, join_type, columns_select)
    return {"query": query}

def insert_in_mongo(data):
    try:
        myclient = pymongo.MongoClient("mongodb://jio_explorer:jioexplorerqa123@10.159.18.75:27017/notebook_db")
        mydb = myclient["datacurator"]
        mycol = mydb["queries"]
        x = mycol.insert_one(data)
        print('[INFO]  Logs written to Mongo with ID=>{}'.format(x.inserted_id))
        return x.inserted_id
    except Exception as e:
        print('[ERROR] mongo error')
        print(e)
        return -1

@app.api_route("/api/v1/data_curator/execute_query", methods=['POST'])
async def execute_sql_query(query: str = Body(..., embed = True), session_id: int = Body(..., embed = True), view_name: str = Body(..., embed=True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    id = insert_in_mongo({
        'username': user_name,
        'timestamp' : datetime.datetime.now(),
        'query': query,
        'status' : "valid"
        })
    if(id == -1):
        return 500
    table_name = user_name.split(".")[0].lower() + "_" + str(id)
    data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        spark.sql('create table network_dev.""" + table_name + """ location "/user/""" + user_name + """/datacurator/""" + table_name + """" as """ + query + """')
        df = spark.sql('select * from network_dev.""" + table_name + """ limit 10')
        df1 = df.toPandas()
        print(json.dumps({'res': df1.to_json(orient="records")}, indent=4))
        df_view_name = '""" + view_name + """_df'
        globals()[df_view_name] = df
        del[[df1]]
        del[[df]]
            """)
        }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)

    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
    return {json.dumps(output)}

@app.api_route("/upload_data_for_clustering", methods=['POST'])
async def upload_data(file_name: UploadFile = File(...)):
    dir = "/workspace/DataCurio"
    perm_dir = "/workspace/userData/DataCurio/"
    upload_dir = os.path.join(dir, 'data')
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(perm_dir, exist_ok=True)
    timestamp = datetime.datetime.now().strftime('%s')
    # file_name = file_name.filename
    fname = file_name.filename.split(".")
    fname = fname[0] + '_' + timestamp + '.' + fname[-1]
    # file_name.save(os.path.join(upload_dir, fname))
    with open(os.path.join(upload_dir, fname), "wb+") as file_object:
        file_object.write(file_name.file.read())
    shutil.copy(upload_dir + "/" + fname, perm_dir + file_name.filename)
    import pandas as pd
    df = pd.read_csv(upload_dir + "/" + fname, nrows=1)
    return {"filepath": perm_dir + file_name.filename,  "columns": df.columns.values.tolist()}


@app.api_route("/api/v1/selected_file_columns", methods=['POST'])
async def select_file_columns(filepath: str= Body(..., embed = True)):
    import pandas as pd
    df = pd.read_csv(filepath, nrows=1)
    return {"filepath": filepath, "columns": df.columns.values.tolist()}

@app.api_route("/api/v1/fe/before_processing", methods=["POST"])
async def data_before_processing(filepath: str= Body(..., embed=True)):
    df = pd.read_csv(filepath, nrows=5)
    return {df.to_json(orient="records")}

@app.api_route("/api/v1/fe/preprocess_pipeline", methods=["POST"])
async def preprocessing_pipeline(preprocess: List[List[str]], filepath: str = Body(..., embed = True)):
    def drop_col(col):
        return (pdp.PdPipeline([pdp.ColDrop(col)]))
    def one_hot_encode(col):
        return  (pdp.PdPipeline([pdp.OneHotEncode(col)]))
    def replace_na_with_mean(col):
        temp_pipeline = pdp.make_pdpipeline()
        temp_pipeline += pdp.PdPipeline([pdp.AggByCols(columns=col, func=np.mean, drop=False, suffix='_mean')])
        temp_pipeline += pdp.PdPipeline([pdp.ApplyToRows(func=lambda row: row[col+'_mean'] if np.isnan(row[col]) else row[col], colname=col+'_repl_mean', follow_column=col+'_mean')])
        temp_pipeline += pdp.ColDrop(col+'_mean')
        return temp_pipeline
    def replace_na_with_median(col):
        temp_pipeline = pdp.make_pdpipeline()
        temp_pipeline += pdp.PdPipeline([pdp.AggByCols(columns=col, func=np.median, drop=False, suffix='_median')])
        temp_pipeline += pdp.PdPipeline([pdp.ApplyToRows(func=lambda row: row[col+'_median'] if np.isnan(row[col]) else row[col], colname=col+'_repl_median', follow_column=col+'_median')])
        temp_pipeline += pdp.ColDrop(col+'_median')
        return temp_pipeline
    def log_transform(col):
        temp_pipeline = pdp.make_pdpipeline()
        temp_pipeline += pdp.PdPipeline([pdp.PdPipeline([pdp.AggByCols(col ,func=np.log)])])
        temp_pipeline += pdp.PdPipeline([pdp.ApplyToRows(func=lambda row: 0 if np.isinf(row[col]) else row[col], colname=col, follow_column=col)])
        return temp_pipeline
    
    pipeline = pdp.make_pdpipeline()
    
    for i in preprocess:
        if(i[1] == "Drop Column"):
            pipeline += drop_col(i[0])
        elif(i[1] == "One-Hot Encoding"):
            pipeline += one_hot_encode(i[0])
        elif(i[1] == "Replace Missing Values with Mean"):
            pipeline += replace_na_with_mean(i[0])
        elif(i[1] == "Replace Missing Values with Median"):
            pipeline += replace_na_with_median(i[0])
        elif(i[1] == "Log Transformation"):
            pipeline += log_transform(i[0])
    
    df = getDataForClustering(filepath)
    df_preprocessed = pipeline.fit_transform(df)
    
    pipeline_path = "/workspace/userData/DataCurio/Pipelines/pipeline_" + str(time.time())
    os.makedirs(pipeline_path, exist_ok=True)
    with open(pipeline_path + "/pipeline.json", "w") as f:
        print(pipeline, file=f)
    
    f = open(pipeline_path + "/pipeline.json")
    pipeline_details = json.dumps(f.read())

    res = df_preprocessed.head(5).to_json(orient="records")
    return {"pipeline_details": pipeline_details, "res": res}

@app.api_route("/api/v1/fe/before_processing_hdfs", methods=['POST'])
async def data_before_processing_hdfs(filepath: str = Body(..., embed = True), session_id: int = Body(..., embed = True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    #filepath = "/shared/dsp/Pratik.Kejriwal/sample_pred_1K.csv"

    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        df =  spark.read.csv( '""" + filepath + """', header=True).limit(5)
        df1 = df.toPandas()
        print(json.dumps({'columns': df1.columns.to_list(), 'sample_rows': df1.to_json(orient="records")}, indent=4))
        del[[df]]
            """)
        }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)

    while current_state != 'available':
        time.sleep(1)
        #print("Waiting for result to be available :",datetime.datetime.now())
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    #print(state_response.json(), file=sys.stderr)
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']

    return {json.dumps(output)}

@app.api_route("/api/v1/fe/df_before_processing_hdfs", methods=['POST'])
async def df_data_before_processing_hdfs(dataframe: str = Body(..., embed = True), session_id: int = Body(..., embed = True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')

    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        df =  (""" + str(dataframe) + """).limit(5)
        df1 = df.toPandas()
        print(json.dumps({'columns': df1.columns.to_list(), 'sample_rows': df1.to_json(orient="records")}, indent=4))
        del[[df]]
            """)
        }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)

    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
    return {json.dumps(output)}

@app.api_route("/api/v1/fe/preprocess_pipeline_hdfs", methods=["POST"])
async def preprocessing_pipeline_hdfs(preprocess: List[List[str]], filepath: str = Body(..., embed = True), session_id: int = Body(..., embed=True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        import pandas as pd
        from pyspark.sql.functions import log10, col
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Imputer
        from pyspark.sql.types import DoubleType
        import os
        df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
        globals()[df_filename] = spark.read.csv( '""" + filepath + """', header=True)
        df = globals()[df_filename]
        preprocess = """ + str(preprocess) + """

        def one_hot_encode(col1):
            temp_stage = []
            stringIndexer = StringIndexer(inputCol = col1, outputCol = col1 + 'Index')
            encoder = OneHotEncoder(inputCol = stringIndexer.getOutputCol(), outputCol = col1 + "classVec")
            temp_stage += [stringIndexer, encoder]
            return temp_stage

        def replace_na_with_mean(col1):
            df = df.withColumn(col1, df[col1].cast(DoubleType()))
            impute = Imputer(inputCols = [col1], outputCols = [col1 + 'repl_mean'])
            impute.setStrategy('mean')
            return impute

        def replace_na_with_median(col1):
            df = df.withColumn(col1, df[col1].cast(DoubleType()))
            impute = Imputer(inputCols = [col1], outputCols = [col1 + 'repl_median'])
            impute.setStrategy('median')
            return impute

        stages = []

        for i in preprocess:
            if(i[1] == "Drop Column"):
                df = df.drop(i[0])
            elif(i[1] == "One-Hot Encoding"):
                stages += one_hot_encode(i[0])
            elif(i[1] == "Replace Missing Values with Mean"):
                stages += replace_na_with_mean(i[0])
            elif(i[1] == "Replace Missing Values with Median"):
                stages += replace_na_with_median(i[0])
            elif(i[1] == "Log Transformation"):
                df = df.withColumn(i[0], df[i[0]].cast(DoubleType()))
                df = df.withColumn(i[0], log10(col(i[0])))
                df = df.fillna(0, subset=[i[0]])

        pipeline = Pipeline(stages = stages)
        pipeline_model = pipeline.fit(df)
        df_processed = pipeline_model.transform(df)
        globals()[df_filename + "_processed"] = df_processed
        df1 = df_processed.limit(5).toPandas()
        print(df1.to_json(orient="records"))
        del[[df]]
        del[[df_processed]]
            """)
          }
          
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)
    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']
    
    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)
    
    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
        
    return {json.dumps(output)}

@app.api_route("/api/v1/fe/df_preprocess_pipeline_hdfs", methods=["POST"])
async def df_preprocessing_pipeline_hdfs(preprocess: List[List[str]], dataframe: str = Body(..., embed = True), session_id: int = Body(..., embed=True)):
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        import pandas as pd
        from pyspark.sql.functions import log10, col
        from pyspark.ml import Pipeline
        from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Imputer
        from pyspark.sql.types import DoubleType

        df =  """ + str(dataframe) + """
        preprocess = """ + str(preprocess) + """

        def one_hot_encode(col1):
            temp_stage = []
            stringIndexer = StringIndexer(inputCol = col1, outputCol = col1 + 'Index')
            encoder = OneHotEncoder(inputCol = stringIndexer.getOutputCol(), outputCol = col1 + "classVec")
            temp_stage += [stringIndexer, encoder]
            return temp_stage

        def replace_na_with_mean(col1):
            df = df.withColumn(col1, df[col1].cast(DoubleType()))
            impute = Imputer(inputCols = [col1], outputCols = [col1 + 'repl_mean'])
            impute.setStrategy('mean')
            return impute

        def replace_na_with_median(col1):
            df = df.withColumn(col1, df[col1].cast(DoubleType()))
            impute = Imputer(inputCols = [col1], outputCols = [col1 + 'repl_median'])
            impute.setStrategy('median')
            return impute

        stages = []

        for i in preprocess:
            if(i[1] == "Drop Column"):
                df = df.drop(i[0])
            elif(i[1] == "One-Hot Encoding"):
                stages += one_hot_encode(i[0])
            elif(i[1] == "Replace Missing Values with Mean"):
                stages += replace_na_with_mean(i[0])
            elif(i[1] == "Replace Missing Values with Median"):
                stages += replace_na_with_median(i[0])
            elif(i[1] == "Log Transformation"):
                df = df.withColumn(i[0], df[i[0]].cast(DoubleType()))
                df = df.withColumn(i[0], log10(col(i[0])))
                df = df.fillna(0, subset=[i[0]])

        pipeline = Pipeline(stages = stages)
        pipeline_model = pipeline.fit(df)
        df_processed = pipeline_model.transform(df)
        # df_name = [x for x in globals() if globals()[x] is """ + str(dataframe) + """][0]
        # df_name + "_processed" = df_processed
        df1 = df_processed.limit(5).toPandas()
        print(df1.to_json(orient="records"))
        del[[df]]
        # del[[df_processed]]
            """)
          }
          
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)
    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']
    
    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)
    
    while current_state != 'available':
        time.sleep(1)
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
        
    return {json.dumps(output)}

def getDataForClustering(filepath):
    import pandas as pd
    #df = pd.read_csv("/workspace/DataCurio/data/" + filename)
    df = pd.read_csv(filepath)
    return df

@app.api_route("/api/v1/clustering", methods=['POST'])
async def getcluster(columns: List[str], params: Dict[str,str], filepath: str = Body(..., embed = True)):
    from pathlib import Path
    # reqJSON = await request.json()
    # params = reqJSON['params']
    # columns = reqJSON['columns']
    # filepath = reqJSON['filepath']

    try :
        #subprocess.getstatusoutput("python clustering_script.py")
        modelName = "clusteringModel_{}".format(time.time())
        process = subprocess.Popen(['python' , 'clustering_script.py', '-f', filepath, '-c', json.dumps(columns), '-p', json.dumps(params), '-m', modelName], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print(out, file=sys.stderr)
    except Exception as e:
        print(e, file=sys.stderr)

    data = getDataForClustering(filepath)

    filename = os.path.basename(filepath)
    output_filename = Path(filename).stem
    output_file = 'label_' + output_filename + ".csv"
    expt_name = Path(os.path.basename(filepath)).stem + "_clustering"
    data_labels = getDataForClustering("/workspace/DataCurio/data/" + output_file)

    data['labels'] = data_labels['labels']
    result = {}
    n_clusters = int(params['n_clusters'])
    for i in range(n_clusters):
        stats = {}
        for col in columns:
            element = {
                "max_value": round(data[ data['labels'] == i ][col].max(), 2),
                "mean_value": round(data[ data['labels'] == i ][col].mean(), 2),
                "min_value" : round(data[ data['labels'] == i ][col].min(), 2)
            }
            stats[col] = element
        result['cluster_' + str(i)] = stats

    params_output_json_file = '/workspace/DataCurio/data/params_' + output_filename + ".json"
    f = open(params_output_json_file)
    used_params = json.load(f)
    return {"used_params" : used_params, "ca_feature_level": result, "modelName": modelName, "expt_name": expt_name}


@app.api_route("/api/v1/optimalclusters", methods=['POST'])
async def getoptimalclusters(columns: List[str], filepath: str = Body(..., embed = True)):

    data = getDataForClustering(filepath)
    wcss = []
    for i in range(1, 11):
        kmeans = KMeans(n_clusters = i, init = 'k-means++', random_state = 42)
        kmeans.fit(data[columns])
        wcss.append(kmeans.inertia_)

    data  = pd.DataFrame(wcss, columns=['WCSS'])
    data['number of clusters'] = [i for i in range(1, 11)]

    fig = px.line(data , x="number of clusters", y="WCSS", title="variance as function of number cluster")
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON



# @app.api_route("/upload_file", methods=["GET", "POST"])
# 
# async def upload_file():
#     if request.method == 'POST':
#         f = request.files['file_name']
#         timestamp = datetime.datetime.now().strftime('%s')
#         fname = f.filename.split(".")
#         fname = fname[0] + '_' + timestamp + '.' + fname[-1]
#         f.save(os.path.join(app.config['UPLOAD_PATH'], fname))
#         import pandas as pd
#         df = pd.read_csv(app.config['UPLOAD_PATH'] + "/" + fname)
#         return templates.TemplateResponse('clustering.html', msg='File has been upload successfully!', col_names=df.columns.values.tolist(), fname=fname)
#     return templates.TemplateResponse("clustering.html", msg="Please upload file")


#@app.api_route("/api/v1/ca_feature_level", methods=["GET", "POST"])
#async def ca_feature_level():
#    filename = request.json['filename']
#    data = getDataForClustering(filename)
#
#    output_filename = Path(filename).stem
#    labels = pd.read_csv('/workspace/DataCurio/data/output_' + output_filename + ".csv")
#    columns = data.columns
#    data['labels'] = labels
#    n_clusters = 3
#    #columns = ["Sepal Length",        "Sepal Width",  "Petal Length", "Petal Width"]
#    result = {}
#    for i in range(n_clusters):
#        stats = {}
#        for col in columns:
#            element = {
#                "max_value": data[ data['labels'] == i ][col].max(),
#                "min_value" : data[ data['labels'] == i ][col].min()
#            }
#            stats[col] = element
#        result['cluster_' + str(i)] = stats
#    return result

# @app.api_route("/api/v1/getclustersize", methods=['POST'])
# async def getclustersizes(filepath: str = Body(..., embed = True)):
#     output_filename = Path(filepath).stem
#     labels = pd.read_csv('/workspace/DataCurio/data/label_' + output_filename + ".csv")

#     newdf = labels.groupby(by=["labels"]).size().reset_index(name="counts")

#     clusters = newdf.shape[0]
#     json_obj = {}
#     for i in range(clusters):
#         json_obj['cluster_' + str(i)] = str( newdf[ newdf['labels'] == i]['counts'].max() )

#     return json_obj

@app.api_route("/api/v1/getclusterpostPCA3d", methods=["POST"])
async def getClusterPostPCA3d(columns: List[str], params: Dict[str,str], filepath: str = Body(..., embed = True)):
    df = getDataForClustering(filepath)
    x = df[columns]

    scaler = StandardScaler()
    df_std = scaler.fit_transform(x)

    pca_transformation = PCA(n_components=3)
    df_pca = pca_transformation.fit_transform(df_std)

    if('n_clusters' in params.keys()):
        params['n_clusters'] = int(params['n_clusters'])
    if('max_iter' in params.keys()):
        params['max_iter'] = int(params['max_iter'])

    model = KMeans()
    model.set_params(**params)
    model.fit(df_pca)

    labels = model.labels_
    df['labels']= labels
    filename = os.path.basename(filepath)
    output_filename = Path(filename).stem
    label_df = df['labels']
    label_df.to_csv('/workspace/DataCurio/data/label_pca_' + output_filename + ".csv", header=True, index=None)

    centers = model.cluster_centers_
    centers_df = pd.DataFrame(centers, columns=['X', 'Y', 'Z'])
    print(centers_df, file=sys.stderr)
    fig = px.scatter_3d(centers_df, x='X', y='Y', z='Z')
    fig.update_traces(marker_size=5)

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

@app.api_route("/api/v1/get_columns_in_csv_from_hdfs", methods=['POST'])
async def getDataForClusteringFromHDFS(filepath: str = Body(..., embed = True), session_id: int = Body(..., embed = True)):
        headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
        auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
        #filepath = "/shared/dsp/Manish10.Verma/Dec_filtered_data.csv"

        session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
        statements_url = session_url+'/statements'
        data = {
          'code': textwrap.dedent("""
            import pprint
            import json
            df =  spark.read.csv( '""" + filepath + """', header=True).limit(1)
            print(json.dumps({'columns':df.columns}, indent=4))
            del[[df]]
               """)
          }
        r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

        r = requests.get(statements_url, headers=headers,auth=auth)
        total_statement = r.json()['statements']
        current_state = r.json()['statements'][len(total_statement)-1]['state']

        if current_state == 'available':
            state_response = requests.get(statements_url, headers=headers,auth=auth)

        while current_state != 'available':
            time.sleep(1)
            #print("Waiting for result to be available :",datetime.datetime.now())
            state_response = requests.get(statements_url, headers=headers,auth=auth)
            current_state = state_response.json()['statements'][len(total_statement)-1]['state']
        #print(state_response.json(), file=sys.stderr)
        output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']

        return {"filepath": filepath,  "columns": json.loads(output)['columns']}

@app.api_route("/api/v1/get_df_columns_from_hdfs", methods=['POST'])
async def getDFColumnNamesfromHDFS(dataframe: str = Body(..., embed = True), session_id: int = Body(..., embed = True)):
        headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
        auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
        
        session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
        statements_url = session_url+'/statements'
        data = {
          'code': textwrap.dedent("""
            import pprint
            import json
            print(json.dumps({'columns':""" + str(dataframe) + """.columns}, indent=4))
               """)
          }
        r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

        r = requests.get(statements_url, headers=headers,auth=auth)
        total_statement = r.json()['statements']
        current_state = r.json()['statements'][len(total_statement)-1]['state']

        if current_state == 'available':
            state_response = requests.get(statements_url, headers=headers,auth=auth)

        while current_state != 'available':
            time.sleep(1)
            state_response = requests.get(statements_url, headers=headers,auth=auth)
            current_state = state_response.json()['statements'][len(total_statement)-1]['state']
        output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
        return {"columns": json.loads(output)['columns']}

# @app.api_route("/api/v1/getclusterpostTSNE3d", methods=["POST"])
# async def getClusterPostTSNE3d(request: Request):
#     reqJSON = await request.json()
#     filename = reqJSON['filename']
#     columns = reqJSON['columns']
#     params = reqJSON['params']
#     df = getDataForClustering(filename)
#     x = df[columns]

#     scaler = StandardScaler()
#     df_std = scaler.fit_transform(x)

#     tsne_transformation = TSNE(n_components=3, init='pca')
#     df_tsne = tsne_transformation.fit_transform(df_std)

#     model = KMeans()
#     model.set_params(**params)
#     model.fit(df_tsne)

#     labels = model.labels_
#     df['labels']= labels

#     output_filename = Path(filename).stem
#     label_df = df['labels']
#     label_df.to_csv('/workspace/DataCurio/data/label_tsne_' + output_filename + ".csv", header=True, index=None)

#     centers = model.cluster_centers_
#     centers_df = pd.DataFrame(centers, columns=['X', 'Y', 'Z'])
#     print(centers_df, file=sys.stderr)
#     fig = px.scatter_3d(centers_df, x='X', y='Y', z='Z')
#     fig.update_traces(marker_size=5)

#     graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
#     return graphJSON

# @app.api_route("/api/v1/getpcaclustersize", methods=['POST'])
# async def getpcaclustersizes(request: Request):
#     reqJSON = await request.json()
#     filename = reqJSON['filename']
#     output_filename = Path(filename).stem
#     labels = pd.read_csv('/workspace/DataCurio/data/label_pca_' + output_filename + ".csv")

#     newdf = labels.groupby(by=["labels"]).size().reset_index(name="counts")

#     clusters = newdf.shape[0]
#     json_obj = {}
#     for i in range(clusters):
#         json_obj['cluster_' + str(i)] = str( newdf[ newdf['labels'] == i]['counts'].max() )

#     return json_obj

@app.api_route("/api/v1/get_optimal_clusters_from_hdfs", methods=['POST'])
async def getOptimalClustersFromHDFS(request: Request):
        reqJSON = await request.json()
        filepath = reqJSON['filepath']
        session_id = reqJSON['session_id']
        columns = reqJSON['columns']

        headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
        auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
        
        session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
        statements_url = session_url+'/statements'
        data = {
          'code': textwrap.dedent("""
            import pprint
            import json
            import plotly.express as px
            import pandas as pd
            import plotly
            from pyspark.ml.clustering import KMeans
            from pyspark.ml.feature import VectorAssembler
            import os
            
            df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
            globals()[df_filename] = spark.read.csv( '""" + filepath + """', inferSchema=True, header=True)
            df = globals()[df_filename]
            vecAssembler = VectorAssembler(inputCols=""" + str(columns) + """, outputCol="features")
            vec_df = vecAssembler.transform(df)

            wcss = []
            for i in range(2, 11):
                kmeans = KMeans().setK(i)
                model = kmeans.fit(vec_df.select('features'))
                wcss.append(model.computeCost(vec_df))

            data  = pd.DataFrame(wcss, columns=['WCSS'])
            data['number of clusters'] = pd.DataFrame([i for i in range(2, 11)])

            fig = px.line(data , x="number of clusters", y="WCSS", title="variance as function of number cluster")
            graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

            print(json.dumps({'graphjson':graphJSON}, indent=4))
            del[[df]]
            del[[vec_df]]
               """)
          }
        r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

        r = requests.get(statements_url, headers=headers,auth=auth)
        total_statement = r.json()['statements']
        current_state = r.json()['statements'][len(total_statement)-1]['state']

        if current_state == 'available':
            state_response = requests.get(statements_url, headers=headers,auth=auth)

        while current_state != 'available':
            time.sleep(1)
            #print("Waiting for result to be available :",datetime.datetime.now())
            state_response = requests.get(statements_url, headers=headers,auth=auth)
            current_state = state_response.json()['statements'][len(total_statement)-1]['state']
        #print(state_response.json(), file=sys.stderr)
        output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
        print(output, file=sys.stderr)
        return {"graphjson": json.loads(output)['graphjson']}

@app.api_route("/api/v1/clusteringonHDFS", methods=['POST'])
async def getclusterHDFS(request: Request):
    from pathlib import Path
    reqJSON = await request.json()
    filepath = reqJSON['filepath']
    session_id = reqJSON['session_id']
    
    params = reqJSON['params']
    columns = reqJSON['columns']
    
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
      'code': textwrap.dedent("""
      import pprint
      import json
      import pandas as pd
      from pyspark.ml.clustering import KMeans
      from pyspark.ml.feature import VectorAssembler
      from pyspark.ml.feature import StandardScaler
      from pyspark.ml.feature import PCA 
      from pyspark.sql import Row
      from pyspark.sql.functions import monotonically_increasing_id
      import os
      df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
      globals()[df_filename] = spark.read.csv( '""" + filepath + """', inferSchema=True, header=True)
      df = globals()[df_filename]

      columns = """ + str(columns) + """
      vecAssembler = VectorAssembler(inputCols= columns, outputCol="f1")
      vec_df = vecAssembler.transform(df)
      
      scale=StandardScaler(inputCol='f1',outputCol='standardized')
      vec_df_scale=scale.fit(vec_df)
      vec_df_scale_output=vec_df_scale.transform(vec_df)
      
      pca = PCA(k=3, inputCol="standardized", outputCol="features")
      pca_model = pca.fit(vec_df_scale_output)
      vec_df_scale_pca_output = pca_model.transform(vec_df_scale_output)
      params = """ + str(params) + """ 
      kmeans = KMeans().setParams(**params)
      model = kmeans.fit(vec_df_scale_pca_output.select('features'))
      centres = model.clusterCenters()
      
      prediction = model.transform(vec_df_scale_pca_output).select('prediction').collect()
      labels = [ p.prediction for p in prediction ]
      tt = spark.sparkContext.parallelize(labels)
      newrow = Row('labels')
      tdf = tt.map(newrow).toDF()
      
      tidf = tdf.select('*').withColumn('id', monotonically_increasing_id())
      new_vec_df = vec_df.select('*').withColumn('id', monotonically_increasing_id())
      
      newdf = new_vec_df.join(tidf, new_vec_df.id == tidf.id, how='inner')
      newdf.createOrReplaceTempView("tmp")
      # need to change as per input table - here for demo purpose
      outputdf = spark.sql("select labels, max(avg_bad_coverage_pct) as max_avg_bad_coverage_pct, mean(avg_bad_coverage_pct) as mean_avg_bad_coverage_pct, min(avg_bad_coverage_pct) as min_avg_bad_coverage_pct, max(avg_bad_voice_pct) as max_avg_bad_voice_pct, mean(avg_bad_voice_pct) as mean_avg_bad_voice_pct, min(avg_bad_voice_pct) as min_avg_bad_voice_pct, max(avg_total_voice_call) as max_avg_total_voice_call, mean(avg_total_voice_call) as mean_avg_total_voice_call, min(avg_total_voice_call) as min_avg_total_voice_call from tmp group by labels")
      
      output_df = outputdf.toPandas()
      
      result = {}
      n_clusters = 4
      for i in range(0, n_clusters):
          stats = {}
          for col in columns:
              element = {
                  "max_value": round(output_df[output_df['labels'] == i]['max_'+col], 2),
                  "mean_value": round(output_df[output_df['labels'] == i]['mean_'+col], 2),
                  "min_value": round(output_df[output_df['labels'] == i]['min_'+col], 2)
              }
              stats[col] = element
          result['cluster_' + str(i)] = stats
        
      print(json.dumps(result))
      del[[df]]
      del[[vec_df]]
      del[[vec_df_scale_output]]
      del[[vec_df_scale_pca_output]]
      del[[prediction]]
      del[[tdf]]
      del[[new_vec_df]]
      del[[newdf]]
      del[[outputdf]]
           """)
      }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)

    while current_state != 'available':
        time.sleep(1)
        #print("Waiting for result to be available :",datetime.datetime.now())
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    #print(state_response.json(), file=sys.stderr)
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
    print(output, file=sys.stderr)
    return {"ca_feature_level": json.loads(output)}

@app.api_route("/api/v1/clustering_post_pca_3d_hdfs", methods=['POST'])
async def getClusteringPostPCA_HDFS(request: Request):
    reqJSON = await request.json()
    filepath = reqJSON['filepath']
    columns = reqJSON['columns']
    params = reqJSON['params']
    
    session_id = reqJSON['session_id']
    
    headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
    auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
    
    session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
    statements_url = session_url+'/statements'
    data = {
      'code': textwrap.dedent("""
        import pprint
        import json
        import plotly.express as px
        import pandas as pd
        import plotly
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.feature import StandardScaler
        from pyspark.ml.feature import PCA 
        import os
        
        df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
        globals()[df_filename] = spark.read.csv( '""" + filepath + """', inferSchema=True, header=True)
        df = globals()[df_filename]
        vecAssembler = VectorAssembler(inputCols=""" + str(columns) + """, outputCol="f1")
        vec_df = vecAssembler.transform(df)
                

        scale=StandardScaler(inputCol='f1',outputCol='standardized')
        vec_df_scale=scale.fit(vec_df)
        vec_df_scale_output=vec_df_scale.transform(vec_df)


        pca = PCA(k=3, inputCol="standardized", outputCol="features")
        pca_model = pca.fit(vec_df_scale_output)
        vec_df_scale_pca_output = pca_model.transform(vec_df_scale_output)

        params = """ + str(params) + """ 
        kmeans = KMeans().setParams(**params)
        model = kmeans.fit(vec_df_scale_pca_output.select('features'))
        centers = model.clusterCenters()
		   
        df = pd.DataFrame(centers, columns=['X', 'Y', 'Z'])
		
        fig = px.scatter_3d(df, x='X', y='Y', z='Z')
        fig.update_traces(marker_size=5)
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
	
        print(json.dumps({'graphjson':graphJSON}, indent=4))
        del[[df]]
        del[[vec_df]]
        del[[vec_df_scale_output]]
        del[[vec_df_scale_pca_output]]
           """)
      }
    r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

    r = requests.get(statements_url, headers=headers,auth=auth)
    total_statement = r.json()['statements']
    current_state = r.json()['statements'][len(total_statement)-1]['state']

    if current_state == 'available':
        state_response = requests.get(statements_url, headers=headers,auth=auth)

    while current_state != 'available':
        time.sleep(1)
        #print("Waiting for result to be available :",datetime.datetime.now())
        state_response = requests.get(statements_url, headers=headers,auth=auth)
        current_state = state_response.json()['statements'][len(total_statement)-1]['state']
    #print(state_response.json(), file=sys.stderr)
    output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
    print(output, file=sys.stderr)
    return {"graphjson": json.loads(output)['graphjson']}



@app.api_route("/api/v1/LogisticRegression", methods=['POST'])
async def get_logistic_regression(columns: List[str], params: Dict[str,str], target: str = Body(..., embed = True), filepath: str = Body(..., embed = True)):
    try:
        from pathlib import Path
        modelName = "LogisticRegressionModel{}".format(time.time())
        process = subprocess.Popen(['python' , 'logistic_regression_script.py', '-f', filepath, '-c', json.dumps(columns), '-t', target, '-p', json.dumps(params), '-m', modelName], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        # print(out, file=sys.stderr)
        if(err):
            raise Exception(err)
        filename = os.path.basename(filepath)
        output_filename = Path(filename).stem
        expt_name = Path(os.path.basename(filepath)).stem + "_modelling"
        output_json_file = '/workspace/DataCurio/data/report_' + output_filename + ".json"
        f = open(output_json_file)
        report = json.load(f)
        return {'report': report, 'modelName': modelName, "expt_name": expt_name}, 200
    except Exception as e:
        # print(e, file=sys.stderr)
        errorString = str(e).split("\\n")
        return {"error":str(errorString[-2])}, 500

@app.api_route("/api/v1/LogisticRegression_HDFS", methods=['POST'])
async def logisticRegressionHDFS(request: Request):
    from pathlib import Path
    reqJSON = await request.json()
    try:
    
        filepath = reqJSON['filepath']
        session_id = reqJSON['session_id']
        params = reqJSON['params']
        train_ratio = (params.pop('trainSplit', 70)) * 0.01
        test_ratio = 1 - train_ratio
        columns = reqJSON['columns']
        target = reqJSON['target']
        
        headers = {'Content-Type': 'application/json','X-Requested-By': 'dev_ds_dsp' }
        auth = HTTPNegotiateAuth(negotiate_client_name='dev_ds_dsp')
        
        session_url = 'http://bdpdata0291.jio.com:8999/sessions/'+str(session_id)
        statements_url = session_url+'/statements'
        data = {
        'code': textwrap.dedent("""
        import pprint
        import json
        import pandas as pd
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.feature import StandardScaler
        from pyspark.ml.feature import StringIndexer
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        import os
        
        df_filename = os.path.splitext(os.path.basename('""" + filepath + """'))[0]
        globals()[df_filename] = spark.read.csv( '""" + filepath + """', inferSchema=True, header=True)
        df = globals()[df_filename]

        feature_encode = VectorAssembler().setInputCols(""" + str(columns) + """).setOutputCol("vec_features")
        vec_df = feature_encode.transform(df)

        label_encode = StringIndexer().setInputCol('""" + target + """').setOutputCol('labels')
        label_model = label_encode.fit(vec_df)
        label_indexed_df = label_model.transform(vec_df)
        
        scale = StandardScaler(inputCol = 'vec_features', outputCol = 'features')
        vec_df_scaler = scale.fit(label_indexed_df)
        vec_scaled_df = vec_df_scaler.transform(label_indexed_df)

        train, test = vec_scaled_df.randomSplit([""" + str(train_ratio) + """, """ + str(test_ratio) + """])
        params = """ + str(params) + """ 

        lr = LogisticRegression(featuresCol = 'features', labelCol = 'labels', predictionCol = 'predictions').setParams(**params)

        lr_model = lr.fit(train)
        test_prediction = lr_model.transform(test)

        model_summary = lr_model.summary
        accuracy_fit_lr = model_summary.accuracy

        evaluator = MulticlassClassificationEvaluator(labelCol = 'labels', predictionCol = 'predictions')
        
        accuracy_lr = evaluator.evaluate(test_prediction, {evaluator.metricName: 'accuracy'})
        f1_lr = evaluator.evaluate(test_prediction, {evaluator.metricName: 'f1'})
        precision_lr = evaluator.evaluate(test_prediction, {evaluator.metricName: 'weightedPrecision'})
        recall_lr = evaluator.evaluate(test_prediction, {evaluator.metricName: 'weightedRecall'})

        result = {
            'fit':{
                'accuracy_score': accuracy_fit_lr
                },
            'predict':{
                'accuracy_score': accuracy_lr,
                'f1_score': f1_lr,
                'precision_score' : precision_lr,
                'recall_score': recall_lr
                }
            }
        
        print(json.dumps({'res':result}, indent=4))
        del[[df]]
        del[[vec_df]]
        del[[label_indexed_df]]
        del[[vec_scaled_df]]
        del[[train]]
        del[[test]]
        del[[test_prediction]]
            """)
        }
        r = requests.post(statements_url, data=json.dumps(data), headers=headers,auth=auth)

        r = requests.get(statements_url, headers=headers,auth=auth)
        total_statement = r.json()['statements']
        current_state = r.json()['statements'][len(total_statement)-1]['state']

        if current_state == 'available':
            state_response = requests.get(statements_url, headers=headers,auth=auth)

        while current_state != 'available':
            time.sleep(1)
            state_response = requests.get(statements_url, headers=headers,auth=auth)
            current_state = state_response.json()['statements'][len(total_statement)-1]['state']
        output = state_response.json()['statements'][len(total_statement)-1]['output']['data']['text/plain']
        print(output, file=sys.stderr)
        return {"report": json.loads(output)['res']}
    
    except Exception as e:
        print(e)
        return e, 500

@app.api_route('/api/v1/ListOfFiles', methods=['GET'])
async def list_of_files(request: Request):
    import glob
    directory = "/workspace/userData"
    files = []
    csv_pathname = directory + "/**/*.csv"
    csv_files = glob.glob(csv_pathname, recursive=True)
    xls_pathname = directory + "/**/*.xls"
    xls_files = glob.glob(xls_pathname, recursive=True)
    xlsx_pathname = directory + "/**/*.xlsx"
    xlsx_files = glob.glob(xlsx_pathname, recursive=True)
    files.append(csv_files)
    files.append(xls_files)
    files.append(xlsx_files)
    flat_list = [item for sublist in files for item in sublist]
    return flat_list

# @app.api_route('/api/v1/ListOfModels', methods=['GET'])
# async def list_of_models(request: Request):
#     directory = "/workspace/userData/DataCurio/savedModels"
#     os.makedirs(directory, exist_ok=True)
#     model_files = os.listdir(directory)
#     return model_files

@app.api_route('/api/v1/getModelMetaData', methods=['POST'])
async def get_model_meta_data(save_modelName: str = Body(..., embed=True)):
    model_path = "/workspace/userData/DataCurio/savedModels/{}".format(save_modelName)
    f = open(model_path + "/metaData.json", "r")
    res = json.load(f)
    return {"metaData": res}

@app.api_route('/api/v1/saveModel', methods=['POST'])
async def save_model_run(modelName: str = Body(..., embed=True), save_modelName: str = Body(..., embed=True)):
    temp_model_dir = "/workspace/DataCurio/savedModels/"
    model_dir = "/workspace/userData/DataCurio/savedModels/"
    os.makedirs(model_dir, exist_ok=True)
    shutil.rmtree(model_dir + modelName, ignore_errors = True)
    shutil.copytree(temp_model_dir + modelName, model_dir + save_modelName)
    return {"saved_model_path" : model_dir + save_modelName}, 200

@app.api_route('/api/v1/renameModel', methods=['POST'])
async def rename_model(old_modelName: str = Body(..., embed=True), new_modelName: str = Body(..., embed=True)):
    directory = "/workspace/userData/DataCurio/savedModels/"
    os.rename(directory + old_modelName, directory + new_modelName)
    return {"new_modelName": new_modelName}, 200

@app.api_route('/api/v1/deleteModel', methods=['POST'])
async def delete_model(modelName: str = Body(..., embed=True)):
    directory = "/workspace/userData/DataCurio/savedModels/"
    shutil.rmtree(directory + modelName)
    return {"deleted_model": modelName}, 200

@app.api_route('/api/v1/ListOfModels', methods=['GET'])
async def list_of_registered_models(request: Request):
    import mlflow
    from mlflow.tracking import MlflowClient
    mlflow.set_tracking_uri("http://localhost:5000")
    client = MlflowClient()
    models = client.list_registered_models()
    model_list = []
    for m in models:
        model_list.append(m.name)
    return {json.dumps(model_list)}

@app.api_route('/api/v1/renameFile', methods=['POST'])
async def rename_file(filepath: str = Body(..., embed=True), newName: str = Body(..., embed=True)):
    parent_dir = Path(filepath).parent.absolute()
    if(Path(newName).suffix):
        os.rename(filepath, os.path.join(parent_dir, newName))
    else:
        suff = Path(filepath).suffix
        newName = newName + suff
        os.rename(filepath, os.path.join(parent_dir, newName))
    return {"newName": newName}, 200

@app.api_route('/api/v1/deleteFile', methods=['POST'])
async def delete_file(filepath: str = Body(..., embed=True)):
    print(filepath, file=sys.stderr)
    os.remove(filepath)
    return {"deleted_file": filepath}, 200


@app.on_event('startup')
def init_data():
    print("Uvicorn Server Init call")
    portname= os.environ['port2900']
    print("portname : ", portname, file=sys.stderr)
    env_folder_path = 'conf/' + env
    print("env folder path : ", env_folder_path, file=sys.stderr)
    global server_obj
    server_obj = livy_session.livy_server(env_folder_path)
    global knox_object
    knox_object = knox_server.knox_data_transfer(os.path.join(env_folder_path,'dsp_config.yml'),"static/","static/")
    return server_obj


if __name__ == "__main__":
    uvicorn.run("__main__:app", host="0.0.0.0", port=2900, reload=True, workers=2, debug=True, log_level="debug")
    
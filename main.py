from dataclasses import field
from urllib import request
from fastapi import Body, FastAPI, Request, UploadFile, File, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
import os
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

app = FastAPI()

templates = Jinja2Templates(directory="templates")

@app.get("/")
def home(request : Request):
    return templates.TemplateResponse("index.html" , {"request" : request})

@app.api_route("/store_file" , methods=['POST'])
def upload_file(file: UploadFile = File(...)):
    # Saves file in the predefined directory
    fname , ext = file.filename.split('.')
    # dir = "./workSpace/userData"
    dir = os.path.join("workSpace" , "userData")
    os.makedirs(dir , exist_ok=True)

    file_url = _save_file_to_disk(file , path = dir , save_as = fname)

    data = pd.read_csv(file_url)
    print(data.head())
    columns_list = data.columns.to_list()
    return {"file_url" : file_url , "columns":columns_list}

@app.api_route("/df_before_processing" , methods=["POST"])
def data_before_processing(filepath: str = Body(...,embed=True)):
    df = pd.read_csv(filepath , nrows=5)
    return {df.to_json(orient="records")}

@app.api_route("/existingFiles" , methods = ["GET"])
def get_list_of_files():
    files_list = list()
    dir = os.path.join("workSpace" , "userData")

    for file in os.listdir(dir):
        if file.endswith(".csv"):
            url = os.path.join(dir , file)
            files_list.append(url)

    return files_list
@app.api_route("/selected_file_columns" , methods=["POST"])
async def get_file_columns(filepath: str = Body(...,embed=True)):
    data = pd.read_csv(filepath)
    return {"columns" : data.columns.to_list()}
@app.api_route("/target_columns" , methods=["POST"])
def get_target_columns(filepath:str = Body(...,embed=True)):
    # Returns possible target columns list
    data = pd.read_csv(filepath)
    numerics = ['int16', 'int32', 'int64']
    data = data.select_dtypes(include=numerics)
    print(data.columns.to_list())
    return {"target-columns" : data.columns.to_list()}

@app.api_route("/feature_selection" , methods = ["POST"])
async def get_analysis(method : str = Body(...,embed=True) , filepath : str = Body(...,embed = True) ,target : str = Body(...,embed = True), kval : str=Body(...,embed = True)):
    kval = int(kval)

    if method.lower() == "chi-square":
        return __chi_square__(filepath , target , kval)
    elif method.lower() == "information gain":
        return __information_gain__(filepath , target , kval)
    elif method.lower() == "fisher":
        return __fisher__(filepath , target , kval)
    elif method.lower() == "correlation coefficient":
        return __correlation_coefficient__(filepath , target , kval)
    else:
        raise HTTPException(
            status_code = 404,
            detail = f"Method : {method} doesn't exist"
        )

# Feature Selection Methods Implementation
def __chi_square__(filepath , target , kval):
    from sklearn.feature_selection import chi2
    
    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]
    f_score = chi2(X,y)

    #First array will return f scores and second array will return p scores

    p_values = pd.Series(f_score[1] , index=X.columns)
    p_values.sort_values(ascending = False , inplace =True)

    k_best_feature = p_values.head(kval).index.to_list()
    #This will select the k best features from the data set.

    v1 = p_values.index.to_list()
    v2 = p_values.to_list()
    print(v1)
    print(v2)
    return {"labels" : v1 , "values": v2 , "best-features" : k_best_feature}

def __information_gain__(filepath , target , kval):
    from sklearn.feature_selection import mutual_info_classif

    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]
    mutual_info = mutual_info_classif(X, y)
    mutual_info = pd.Series(mutual_info)
    print("mutualkl")

    mutual_info.index = X.columns
    print(mutual_info)
    mutual_info.sort_values(ascending=False,inplace=True)

    v1 = mutual_info.index.tolist()
    v2 = mutual_info.values.tolist()
    ans = mutual_info.head(kval).index.tolist()

    return {"labels" : v1 , "values": v2 , "best-features" : ans}

def __correlation_coefficient__(filepath , target , kval):
    data = pd.read_csv(filepath)

    corr_mat = data.corr()
    print("correlation")
    print(corr_mat)
    cor_target = abs(corr_mat[target])
    cor_target = cor_target.sort_values(ascending=False)

    cor_target = cor_target.drop(target)
    kbest_features_col = cor_target.head(kval).index.to_list()
    kbest_features_val = cor_target.head(kval).values.tolist()

    # mat_val = corr_mat.values
    
    N = len(corr_mat.values)
    print(N)
    mat_val = []
    print(mat_val)
    for i in range(N - 1 , -1 , -1):
        print(f"i:{i}" , end="")
        mat_val.append([])
        for j in range(N):
            if i >= j:
                # upper triangle
                mat_val[N - i - 1].append(corr_mat.values[i][j])

    # print(mat_val)
    for i in range(N):
        print(mat_val[i])
    # mat_val = mat_val.tolist()
    return {"best-features":kbest_features_col ,"best-features-val":kbest_features_val ,"corr_mat" : mat_val}

def __fisher__(filepath , target , kval):
    from skfeature.function.similarity_based import fisher_score
    
    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]
    nX = X[X.columns.to_list()].to_numpy()
    ny = y.to_numpy()
    
    print("before")
    ranks = fisher_score.fisher_score(nX , ny)
    print("after")
    print(data.columns[0 : len(data.columns) - 1])
    
    feat_imp = pd.Series(ranks , X.columns)
    feat_imp = feat_imp.sort_values(ascending=False)
    kbest_feature_col = feat_imp.index.tolist()
    kbest_feature_val = feat_imp.values.tolist()
    print(feat_imp)
    print(kbest_feature_col)
    print(kbest_feature_val)

    return {"best-features":kbest_feature_col[:kval] , "labels" : kbest_feature_col , "values": kbest_feature_val}
# Helper functions
def _save_file_to_disk(uploaded_file , path = "." , save_as = "default"):
    # saves file at the given path and returns its complete URL

    extension = os.path.splitext(uploaded_file.filename)[-1]
    print(f"extension: {extension}")
    temp_file = os.path.join(path , save_as + extension)
    print(f"tempfile {temp_file}")
    with open(temp_file , "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file , buffer)
    return temp_file
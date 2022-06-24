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
async def upload_file(file: UploadFile = File(...)):
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
async def data_before_processing(filepath: str = Body(...,embed=True)):
    df = pd.read_csv(filepath , nrows=5)
    return {df.to_json(orient="records")}

@app.api_route("/existingFiles" , methods = ["GET"])
async def get_list_of_files():
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
async def get_target_columns(filepath:str = Body(...,embed=True)):
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
        return await __chi_square__(filepath , target , kval)
    elif method.lower() == "information gain":
        return await __information_gain__(filepath , target , kval)
    elif method.lower() == "fisher":
        return await __fisher__(filepath , target , kval)
    elif method.lower() == "correlation coefficient":
        return await __correlation_coefficient__(filepath , target , kval)
    else:
        raise HTTPException(
            status_code = 404,
            detail = f"Method : {method} doesn't exist"
        )

# Feature Selection Methods Implementation
async def __chi_square__(filepath , target , kval):
    from sklearn.feature_selection import chi2
    from scipy.stats import chi2_contingency

    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    # print(data.head())
    y = data[target]

    ### new implementation

    feature_cols = X.columns
    p_value_list = list()
    statvalue_list = list()
    for col in feature_cols:
        contingency_table = pd.crosstab(X[col] , y)
        contingency_table_val = contingency_table.values
        print(contingency_table)
        stat, pvalue, dof, _ = chi2_contingency(contingency_table_val)
        p_value_list.append(pvalue)

    p_values_df = pd.Series(p_value_list , index=feature_cols)
    p_values_df.sort_values(ascending = True , inplace =True)
    print(p_values_df.head())
    k_best_feature = p_values_df.head(kval).index.to_list()

    v1 = p_values_df.index.to_list()
    v2 = p_values_df.to_list()
    ###
    # f_score = chi2(X,y)
    # print(X.columns)
    # print(f_score[0])
    # #First array will return f scores and second array will return p scores

    # p_values = pd.Series(f_score[1] , index=X.columns)
    # p_values.sort_values(ascending = True , inplace =True)

    # k_best_feature = p_values.head(kval).index.to_list()
    # #This will select the k best features from the data set.

    # v1 = p_values.index.to_list()
    # v2 = p_values.to_list()
    # print(v1)
    # print(v2)

    # stat_val = pd.Series(f_score[0] , index=X.columns)
    # stat_val.sort_values(ascending=False , inplace=True)
    # print(stat_val)

    # k_best_feature = stat_val.head(kval).index.to_list()

    # v1 = stat_val.index.to_list()
    # v2 = stat_val.to_list()
    return {"labels" : v1 , "values": v2 , "best-features" : k_best_feature}

async def __information_gain__(filepath , target , kval):
    from sklearn.feature_selection import mutual_info_classif

    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]
    mutual_info = mutual_info_classif(X, y)
    mutual_info = pd.Series(mutual_info)

    mutual_info.index = X.columns
    mutual_info.sort_values(ascending=False,inplace=True)

    v1 = mutual_info.index.tolist()
    v2 = mutual_info.values.tolist()
    ans = mutual_info.head(kval).index.tolist()

    return {"labels" : v1 , "values": v2 , "best-features" : ans}

async def __correlation_coefficient__(filepath , target , kval):
    data = pd.read_csv(filepath)

    corr_mat = data.corr()
    cor_target = abs(corr_mat[target])
    cor_target = cor_target.sort_values(ascending=False)

    cor_target = cor_target.drop(target)
    kbest_features_col = cor_target.head(kval).index.to_list()
    kbest_features_val = cor_target.head(kval).values.tolist()

    # mat_val = corr_mat.values
    
    N = len(corr_mat.values)
    mat_val = []
    for i in range(N - 1 , -1 , -1):
        mat_val.append([])
        for j in range(N):
            if i >= j:
                # upper triangle
                mat_val[N - i - 1].append(corr_mat.values[i][j])

    return {"best-features":kbest_features_col ,"best-features-val":kbest_features_val ,"corr_mat" : mat_val}

async def __fisher__(filepath , target , kval):
    from skfeature.function.similarity_based import fisher_score
    
    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]
    nX = X[X.columns.to_list()].to_numpy()
    ny = y.to_numpy()
    
    ranks = fisher_score.fisher_score(nX , ny)
    
    feat_imp = pd.Series(ranks , X.columns)
    feat_imp = feat_imp.sort_values(ascending=False)
    kbest_feature_col = feat_imp.index.tolist()
    kbest_feature_val = feat_imp.values.tolist()

    return {"best-features":kbest_feature_col[:kval] , "labels" : kbest_feature_col , "values": kbest_feature_val}
# Helper functions
async def _save_file_to_disk(uploaded_file , path = "." , save_as = "default"):
    # saves file at the given path and returns its complete URL

    extension = os.path.splitext(uploaded_file.filename)[-1]
    temp_file = os.path.join(path , save_as + extension)
    with open(temp_file , "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file , buffer)
    return temp_file
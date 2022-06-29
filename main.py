from dataclasses import field
from glob import glob
import py_compile
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


spark = ""

df_pyspark = "" # global df so that we need not read everytime

@app.get("/")
def home(request : Request):
    global df_pyspark
    if spark != "":
        spark.stop()
    df_pyspark = "" # reset pyspark df to empty
    return templates.TemplateResponse("index_python.html" , {"request" : request})

@app.get("/pyspark")
def launch_pyspark(request: Request):
    start_pyspark_session()
    return templates.TemplateResponse("index_pyspark.html" , {"request" : request})

@app.api_route('/featureSelection_python',methods=['GET'], response_class=HTMLResponse)
async def monitor(request: Request):
    return templates.TemplateResponse('feature_selection_python.html', {"request": request, "host": host, "port": port})

@app.api_route("/store_file" , methods=['POST'])
async def upload_file(file: UploadFile = File(...)):
    # Saves file in the predefined directory
    fname , ext = file.filename.split('.')

    dir = os.path.join("workSpace" , "userData")
    os.makedirs(dir , exist_ok=True)

    file_url = await _save_file_to_disk(file , path = dir , save_as = fname)

    columns_list = await file_columns(file_url)
    return {"file_url" : file_url , "columns":columns_list}


def start_pyspark_session():
    global spark
    import sys
    from pyspark.sql import SparkSession

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = SparkSession.builder.getOrCreate()

@app.api_route("/df_before_processing" , methods=["POST"])
async def data_before_processing(filepath: str = Body(...,embed=True)):
    df = pd.read_csv(filepath , nrows=5)
    return {df.to_json(orient="records")}

@app.api_route("/data_values_pyspark" , methods=["POST"])
async def load_df_and_return_vals_pyspark(filepath: str = Body(...,embed=True)):
    global df_pyspark, spark
    display_entries = 5 #default values
    df_pyspark = spark.read.csv(filepath , header=True,inferSchema=True)
    #cleaning the df_pyspark, since pyspark doesn't supoort '.' in column names
    df_pyspark = df_pyspark.toDF(*(c.replace('.', '_') for c in df_pyspark.columns))

    df_for_display = df_pyspark.toPandas().head(display_entries)
    # print(df.to_json(orient="records"))
    return {df_for_display.to_json(orient="records")}

async def file_columns(file_url):
    import csv
    columns_list = list()

    with open(file_url) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        for row in csv_reader:
            columns_list = row
            break
    return columns_list

@app.api_route("/existing_files" , methods = ["GET"])
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
    columns_list = await file_columns(filepath)

    return {"columns" : columns_list}

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

@app.api_route("/feature_selection_pyspark" , methods = ["POST"])
async def get_analysis(method : str = Body(...,embed=True) , filepath : str = Body(...,embed = True) ,target : str = Body(...,embed = True), kval : str=Body(...,embed = True)):
    kval = int(kval)

    if method.lower() == "chi-square":
        return await __chi_square_pyspark__(filepath , target , kval)
    elif method.lower() == "variance selector":
        return await __variance_selector_pyspark__(filepath , target , kval)
    elif method.lower() == "one way anova":
        return await __one_way_anova_pyspark__(filepath , target , kval)
    elif method.lower() == "correlation coefficient":
        return await __correlation_coefficient_pyspark__(filepath , target , kval)
    else:
        raise HTTPException(
            status_code = 404,
            detail = f"Method : {method} doesn't exist"
        )

# Feature Selection Methods Implementation in Python
async def __chi_square__(filepath , target , kval):
    from scipy.stats import chi2_contingency

    data = pd.read_csv(filepath)
    X = data.drop(target , axis=1)
    y = data[target]

    feature_cols = X.columns
    p_value_list = list()
    statvalue_list = list()
    for col in feature_cols:
        contingency_table = pd.crosstab(X[col] , y)
        contingency_table_val = contingency_table.values
        # print(contingency_table)
        stat, pvalue, _ , _ = chi2_contingency(contingency_table_val)
        statvalue_list.append(stat)

    stat_values_df = pd.Series(statvalue_list , index=feature_cols)
    stat_values_df.sort_values(ascending = False , inplace =True)
    k_best_feature = stat_values_df.head(kval).index.to_list()

    v1 = stat_values_df.index.to_list()
    v2 = stat_values_df.to_list()

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

### Feature selection methods implementation in pyspark

async def __variance_selector_pyspark__(filepath , target , kval):
    # returns features with their variances, and best features based on highest variance 
    global df_pyspark
    var_list = list()
    df_col = df_pyspark.columns
    try:
        df_col.remove(target)
    except:
        print("Target not in list")

    for col in df_col:
        var = df_pyspark.agg({col:"variance"}).collect()[0][0]
        var_list.append(var)
    
    var_pd_series = pd.Series(var_list , index=df_col)
    var_pd_series.sort_values(ascending=False , inplace=True)

    v1 = var_pd_series.index.tolist()
    v2 = var_pd_series.values.tolist()
    kbest_features_col = var_pd_series.head(kval).index.tolist()

    return {"best-features" :kbest_features_col , "labels" : v1 , "values" : v2}

async def __chi_square_pyspark__(filepath , target , kval):
    from pyspark.ml.stat import ChiSquareTest
    from pyspark.ml.feature import VectorAssembler
    global df_pyspark

    df_col = df_pyspark.columns
    try:    
        df_col.remove(target)
    except:
        print("No target present")
    assembler = VectorAssembler(inputCols =  df_col , outputCol= "vector-features")
    vectorized_df = assembler.transform(df_pyspark).select(target , "vector-features")    
    results = ChiSquareTest.test(vectorized_df , "vector-features" , target).head()

    stat_value_list = results.statistics
    stat_value_df = pd.Series(stat_value_list , index = df_col)
    stat_value_df.sort_values(ascending=False , inplace=True)
    k_best_feature = stat_value_df.head(kval).index.to_list()

    v1 = stat_value_df.index.to_list()
    v2 = stat_value_df.to_list()

    return {"labels" : v1 , "values": v2 , "best-features" : k_best_feature}

async def __correlation_coefficient_pyspark__(filepath , target , kval):
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.stat import Correlation
    global df_pyspark

    vector_col = "corr-features"
    assembler = VectorAssembler(inputCols= df_pyspark.columns, outputCol=vector_col)
    df_vector =assembler.transform(df_pyspark).select(vector_col)
    matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
    corrmatrix=matrix.toArray().tolist()
    columns=df_pyspark.columns
    df_corr=spark.createDataFrame(corrmatrix, columns)
    corr_mat = df_corr.toPandas()

    cor_target = abs(corr_mat[target])
    cor_target.index = columns

    cor_target = cor_target.drop(target)
    cor_target = cor_target.sort_values(ascending=False)

    kbest_features_col = cor_target.head(kval).index.to_list()
    kbest_features_val = cor_target.head(kval).values.tolist() 
    N = len(corr_mat.values)
    mat_val = []
    for i in range(N - 1 , -1 , -1):
        mat_val.append([])
        for j in range(N):
            if i >= j:
                mat_val[N - i - 1].append(corr_mat.values[i][j])

    # print(mat_val)
    return {"best-features":kbest_features_col ,"best-features-val":kbest_features_val ,"corr_mat" : mat_val}

async def __one_way_anova_pyspark__(filepath , target , kval):
    from pyspark.sql.functions import lit, avg, count, udf, struct, sum
    from pyspark.sql.types import DoubleType

    global df_pyspark

    def one_way_anova(df, categorical_var, continuous_var):
        """
        Given a Spark Dataframe, compute the one-way ANOVA using the given categorical and continuous variables.
        :param df: Spark Dataframe
        :param categorical_var: Name of the column that represents the grouping variable to use
        :param continuous_var: Name of the column corresponding the continuous variable to analyse
        :return: Sum of squares within groups, Sum of squares between groups, F-statistic, degrees of freedom 1, degrees of freedom 2
        """

        global_avg = df.select(avg(continuous_var)).take(1)[0][0]

        avg_in_groups = df.groupby(categorical_var).agg(avg(continuous_var).alias("Group_avg"),
                                                        count("*").alias("N_of_records_per_group"))
        avg_in_groups = avg_in_groups.withColumn("Global_avg",lit(global_avg))

        udf_between_ss = udf(lambda x: x[0] * (x[1] - x[2]) ** 2,DoubleType())
        between_df = avg_in_groups.withColumn("squared_diff",udf_between_ss(struct('N_of_records_per_group','Global_avg','Group_avg')))
        ssbg = between_df.select(sum('squared_diff')).take(1)[0][0]

        within_df_joined = avg_in_groups \
            .join(df,
                df[categorical_var] == avg_in_groups[categorical_var]) \
            .drop(avg_in_groups[categorical_var])

        udf_within_ss = udf(lambda x: (x[0] - x[1]) ** 2, DoubleType())
        within_df_joined = within_df_joined.withColumn("squared_diff",udf_within_ss(struct(continuous_var,'Group_avg')))
        sswg = within_df_joined \
            .groupby(categorical_var) \
            .agg(sum("squared_diff").alias("sum_of_squares_within_gropus")) \
            .select(sum('sum_of_squares_within_gropus')).take(1)[0][0]
        m = df.groupby(categorical_var) \
            .agg(count("*")) \
            .count()  # number of levels
        n = df.count()  # number of observations
        df1 = m - 1
        df2 = n - m
        f_statistic = (ssbg / df1) / (sswg / df2)
        return sswg, ssbg, f_statistic, df1, df2

    f_stat_list = list()
    df_col = df_pyspark.columns
    try:
        df_col.remove(target)
    except:
        print("Target not in list")

    for col in df_col:
        if col==target:
            continue
        sswg, ssbg, f_statistic, df1, df2 = one_way_anova(df_pyspark, target,col)
        f_stat_list.append(f_statistic)

    stat_series = pd.Series(f_stat_list , index = df_col)
    stat_series = stat_series.sort_values(ascending=False)
    
    k_best_feature = stat_series.head(kval).index.to_list()
    v1 = stat_series.index.to_list()
    v2 = stat_series.to_list()

    return {"labels" : v1 , "values": v2 , "best-features" : k_best_feature}

# Helper functions
async def _save_file_to_disk(uploaded_file , path = "." , save_as = "default"):
    # saves file at the given path and returns its complete URL

    extension = os.path.splitext(uploaded_file.filename)[-1]
    temp_file = os.path.join(path , save_as + extension)
    with open(temp_file , "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file , buffer)
    return temp_file

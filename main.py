from dataclasses import field
from glob import glob
import textwrap
from unicodedata import category
from urllib import request
from fastapi import Body, FastAPI, Query, Request, UploadFile, File, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse
import os
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import List
from fastapi.staticfiles import StaticFiles
import sys

app = FastAPI()

templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")

spark = ""
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.getOrCreate()

@app.get("/")
def home(request : Request):
    return templates.TemplateResponse("index.html" , {"request" : request})

@app.api_route("/upload_data_for_clustering", methods=['POST'])
async def upload_data(file_name: UploadFile = File(...)):
    # Saves file in the predefined directory
    fname , ext = file_name.filename.split('.')
    # dir = "./workSpace/userData"
    dir = os.path.join("workSpace" , "userData")
    os.makedirs(dir , exist_ok=True)

    file_url = await _save_file_to_disk(file_name , path = dir , save_as = fname)

    data = pd.read_csv(file_url)
    print(data.head())
    columns_list = data.columns.to_list()
    return {"filepath": file_url,  "columns": data.columns.values.tolist()}


@app.api_route("/api/v1/selected_file_columns_pyspark", methods=['POST'])
async def select_file_columns(filepath: str= Body(..., embed = True)):
    # implement this in pyspark
    import pandas as pd
    df = pd.read_csv(filepath, nrows=1)
    return {"filepath": filepath, "columns": df.columns.values.tolist()}

@app.api_route('/api/v1/ListOfFiles', methods=['GET'])
async def list_of_files(request: Request):
    import glob
    directory = "/workspace/userData"
    files = []
    csv_pathname = directory + "/**/*.csv"
    print(csv_pathname)
    cwd = os.getcwd()
    print(cwd)
    csv_pathname = cwd + csv_pathname
    csv_files = glob.glob(csv_pathname, recursive=True)
    
    xls_pathname = directory + "/**/*.xls"
    xls_files = glob.glob(xls_pathname, recursive=True)
    xlsx_pathname = directory + "/**/*.xlsx"
    xlsx_files = glob.glob(xlsx_pathname, recursive=True)
    files.append(csv_files)
    files.append(xls_files)
    files.append(xlsx_files)
    flat_list = [item for sublist in files for item in sublist]
    print(flat_list)
    return flat_list

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



@app.api_route("/feature_selection_valid_columns_pyspark" , methods=["POST"])
async def feature_selection_valid_columns_pyspark(filepath:str=Body(...,embed=True)):
    val = await valid_file_columns_pyspark(filepath)
    
    out = dict()
    out["numeric_columns"] = val["numeric_columns"]
    out["categorical_columns"] = val["categorical_columns"]
    out["integer_columns"] = val['integer_columns']
    return out

async def get_int_columns_pyspark(data):
  cols_and_types = data.dtypes
  int_columns = list()
  for c_t in cols_and_types:
    if c_t[1] == 'int':
      int_columns.append(c_t[0])

  return int_columns

async def get_numeric_columns_pyspark(data):
  NUMERIC_COLS = ["int","double"]
  numeric_columns = list()
  for c_t in data.dtypes:
    if c_t[1] in NUMERIC_COLS:
      numeric_columns.append(c_t[0])
  return numeric_columns

async def get_string_columns_pyspark(data):
    # returns string type data
    cols_and_types = data.dtypes
    string_columns = list()
    for c_t in cols_and_types:
        if c_t[1] == 'string':
            string_columns.append(c_t[0])
    return string_columns

async def is_categorical_pyspark(data , col_):
    from pyspark.sql.functions import col, countDistinct

    THRESHOLD_VALUE = 15/100

    if data.select(col_).dtypes[0][1] not in ["string","int"]:
        return False

    uniq = data.agg(countDistinct(col(col_)).alias("count")).collect()[0][0]
    tvalues = data.count()
    threshold_count = THRESHOLD_VALUE * tvalues

    if uniq < threshold_count:
        return True
    return False

async def map_categorical_to_numeric_pyspark(data , col):
    from pyspark.ml.feature import StringIndexer

    indexer = StringIndexer(inputCol=col, outputCol="categoryIndex")
    new_df = data.select(col)
    indexed = indexer.fit(new_df).transform(new_df)
    output_df = indexed.select("categoryIndex")
    output_df.show()
    return output_df
  
async def valid_file_columns_pyspark(filepath):
    data = spark.read.csv(filepath , header=True , inferSchema=True)
    print(data.dtypes)

    integer_columns = await get_int_columns_pyspark(data)
    numeric_columns = await get_numeric_columns_pyspark(data)
    string_columns = await get_string_columns_pyspark(data)
    print(integer_columns)
    print(numeric_columns)
    print(string_columns)
    cat_candidate_columns = [*string_columns , *integer_columns]
    categorical_cols = list()
    for col in cat_candidate_columns:
        if await is_categorical_pyspark(data , col):
            categorical_cols.append(col)

    print(categorical_cols)
    return {"numeric_columns":numeric_columns , "categorical_columns":categorical_cols,"integer_columns":integer_columns}
    # print("mapping of categorical")
    # for col in categorical_cols:
    #     print("column : " , col)
    #     map_categorical_to_numeric_pyspark(data , col)

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

    df = spark.read.csv(filepath , inferSchema=True , header=True)
    df_col = df.columns
    try:    
        df_col.remove(target)
    except:
        print("No target present")
    
    assembler = VectorAssembler(inputCols =  df_col , outputCol= "vector-features")
    vectorized_df = assembler.transform(df).select("Class" , "vector-features")    
    results = ChiSquareTest.test(vectorized_df , "vector-features" , "Class").head()

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
    
    df = spark.read.csv(filepath , inferSchema=True, header= True)
    vector_col = "corr-features"
    assembler = VectorAssembler(inputCols= df.columns, outputCol=vector_col)
    df_vector =assembler.transform(df).select(vector_col)
    matrix = Correlation.corr(df_vector, vector_col).collect()[0][0]
    corrmatrix=matrix.toArray().tolist()
    columns=df.columns
    df_corr=spark.createDataFrame(corrmatrix, columns)
    corr_mat = df_corr.toPandas()

    cor_target = abs(corr_mat[target])
    cor_target.index=df.columns

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
        avg_in_groups = avg_in_groups.withColumn("Global_avg",
                                                lit(global_avg))

        udf_between_ss = udf(lambda x: x[0] * (x[1] - x[2]) ** 2,
                            DoubleType())
        between_df = avg_in_groups.withColumn("squared_diff",
                                            udf_between_ss(struct('N_of_records_per_group',
                                                                    'Global_avg',
                                                                    'Group_avg')))
        ssbg = between_df.select(sum('squared_diff')).take(1)[0][0]

        within_df_joined = avg_in_groups \
            .join(df,
                df[categorical_var] == avg_in_groups[categorical_var]) \
            .drop(avg_in_groups[categorical_var])

        udf_within_ss = udf(lambda x: (x[0] - x[1]) ** 2, DoubleType())
        within_df_joined = within_df_joined.withColumn("squared_diff",
                                                    udf_within_ss(struct(continuous_var,
                                                                            'Group_avg')))
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

    df = spark.read.csv(filepath , header=True,inferSchema = True)
    f_stat_list = list()
    df_col = df.columns
    try:
        df_col.remove(target)
    except:
        print("Target not in list")
    for col in df_col:
        if col==target:
            continue
        sswg, ssbg, f_statistic, df1, df2 = one_way_anova(df, target,col)
        f_stat_list.append(f_statistic)

    stat_series = pd.Series(f_stat_list , index = df_col)

    stat_series = stat_series.sort_values(ascending=False)
    
    k_best_feature = stat_series.head(kval).index.to_list()

    v1 = stat_series.index.to_list()
    v2 = stat_series.to_list()

    return {"labels" : v1 , "values": v2 , "best-features" : k_best_feature}


async def _save_file_to_disk(uploaded_file , path = "." , save_as = "default"):
    # saves file at the given path and returns its complete URL
    extension = os.path.splitext(uploaded_file.filename)[-1]
    temp_file = os.path.join(path , save_as + extension)
    with open(temp_file , "wb") as buffer:
        shutil.copyfileobj(uploaded_file.file , buffer)
    return temp_file

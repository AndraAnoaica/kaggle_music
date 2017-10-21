#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 19:49:41 2017

@author: andraa
"""

import pandas as pd
from pymongo import MongoClient
import sys
import numpy as np
import pickle
import gc
from datetime import datetime

# function that fill convert the date from 20160607 format into datetime
def convert_date(x):
    x = str(x)
    date = datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]))
    return date

# establish database connection
client = MongoClient("localhost", 27017)
db = client.WSDM
collection = db.user_logs

# read list of members that are to be extracted from the database
members_list_path = "/media/andraa/10160545101605452/kaggle/WSDM-kaggle/intermediate_data/members_list.pkl"
with open(members_list_path, 'rb') as f:
    members_list = pickle.load(f)
    
# set an empty df that will contain features to be trained on  
df_features = pd.DataFrame()
df_features_output_path = "/media/andraa/10160545101605452/kaggle/WSDM-kaggle/intermediate_data/df_features.csv"

# set a contor to see the progress 
k=0
for member_msno in members_list:
    # get the member usage log
    k+=1
    if k%10000 == 0:
        print("user number", k)
    print()
    cursor = collection.find({"msno" : {"$eq" : member_msno}}, 
                         {"_id":0, 
                         "msno":1,
                         "date":1,
                         "num_25":1,
                         "num_50":1,
                         "num_75":1,
                         "num_985":1,
                         "num_100":1,
                         "num_unq":1,
                         "total_secs":1})
    # store the logs into a dataframe
    df_user = pd.DataFrame(list(cursor))    
    # change the date format
    df_user["date"] = df_user["date"].apply(lambda x: convert_date(x))
    # set date as index
    df_user.set_index("date", inplace=True)
    #see for the particular user how many empty days we have
    min_timestamp = df_user.index[0]
    max_timestamp = df_user.index[-1]
    idx = pd.date_range(min_timestamp, max_timestamp)
    df_user = df_user.reindex(idx, fill_value = 0) 
    # we are counting the missing days per month
    df_user_null = df_user.groupby(["msno", pd.TimeGrouper(freq = "M")]).count().loc[0,]["num_100"].to_frame()
    df_user_null.columns = ["nb_days_no_music_month"]
    # do an aggregation by month of all the varibles
    df_user_month = df_user.groupby([pd.TimeGrouper(freq = "M")]).sum()
    df_user_merge = pd.concat([df_user_month, df_user_null], axis = 1)
    del df_user_month, df_user_null, df_user
    gc.collect()
    # we are only selecting the users that have been using the service for 
    # more than 3 months
    if len(df_user_merge >=3):
        # select only the last 3 months
        df_user_3_month = df_user_merge.tail(3)
        # do the mean on the whole period except the last three months
        df_user_mean = df_user_merge[:-3].mean()
        # see behaviour changes in the last 3 months
        for column in list(df_user_3_month.columns.values):
            df_user_3_month[column] = df_user_3_month[column].apply(lambda x: \
                           x*100/df_user_mean[column])
        # reshape so you obtain only one line with all the features    
        df_user_3_month = df_user_3_month.values.reshape(1,24)
        df_user_3_month.columns = ["num_100_1", "num_25_1", "num_50_1", "num_75_1", 
                           "num_985_1", "num_unq_1", "total_secs_1", 
                           "nb_days_no_music_month_1",
                           "num_200_2", "num_25_2", "num_50_2", "num_75_2",
                           "num_985_2", "num_unq_2", "total_secs_2", 
                           "nb_days_no_music_month_2", "num_300_3", "num_35_3", 
                           "num_50_3", "num_75_3", "num_985_3", "num_unq_3",
                           "total_secs_3", "nb_days_no_music_month_3" ]
        # set the member msno as index
        df_user_3_month["msno"] = member_msno
        df_features = pd.concat([df_user_3_month["msno"]])
df_features.to_csv(df_features_output_path)

    
    
    
    
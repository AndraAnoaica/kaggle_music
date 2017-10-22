#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 19:10:33 2017

@author: andraa
"""

import pandas as pd
from datetime import datetime
import pickle
import time

# read the user logs
user_logs = pd.read_csv("/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/user_log_members.csv",
                        chunksize = 100000, iterator = True)
# adress to save output
user_logs_dict_path = ("/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/user_logs_dict.pkl")
# function that fill convert the date from 20160607 format into datetime
def convert_date(x):
    x = str(x)
    date = datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]))
    return date

# create empty dictionary for members and aggregated attributes
user_logs_dict = {}
# create set with all the keys
user_logs_dict_keys = set()

# variable that sees the chunk where we are doing the computation
cont = 0
start_time = time.time()
# iterate through user_logs
for chunk in user_logs:
    
    chunk["date"] = chunk["date"].apply(lambda x: convert_date(x))
    """
    for index, row in chunk.iterrows():
        if row["msno"] not in user_logs_dict_keys:
            user_logs_dict_keys.add(row["msno"])
            user_logs_dict[row["msno"]] = {"num_25_sum" : row["num_25"],
                                           "num_50_sum" : row["num_50"],
                                           "num_75_sum" : row["num_75"],
                                           "num_985_sum" : row["num_985"],
                                           "num_100_sum" : row["num_100"],
                                           "num_unq_sum" : row["num_unq"],
                                           "total_secs_sum" : row["total_secs"],
                                           "date_list": [row["date"]],
                                           "nb_records" : 1
                
                }
        else: 
            user_logs_dict[row["msno"]]["num_25_sum"] = user_logs_dict[row["msno"]]["num_25_sum"] + row["num_25"]
            user_logs_dict[row["msno"]]["num_50_sum"] = user_logs_dict[row["msno"]]["num_50_sum"] + row["num_50"]
            user_logs_dict[row["msno"]]["num_75_sum"] = user_logs_dict[row["msno"]]["num_75_sum"] + row["num_75"]
            user_logs_dict[row["msno"]]["num_985_sum"] = user_logs_dict[row["msno"]]["num_985_sum"] + row["num_985"]
            user_logs_dict[row["msno"]]["num_100_sum"] = user_logs_dict[row["msno"]]["num_100_sum"] + row["num_100"]
            user_logs_dict[row["msno"]]["num_unq_sum"] = user_logs_dict[row["msno"]]["num_unq_sum"] + row["num_unq"]
            user_logs_dict[row["msno"]]["total_secs_sum"] = user_logs_dict[row["msno"]]["total_secs_sum"] + row["total_secs"]
            user_logs_dict[row["msno"]]["date_list"].append(row["date"])
            user_logs_dict[row["msno"]]["nb_records"] = user_logs_dict[row["msno"]]["nb_records"] + 1
    """
    chunk_sum = chunk.groupby("msno").sum()
    chunk_timestamp_list = chunk.groupby("msno")["date"].apply(list).to_frame()
    chunk_group = pd.merge(chunk_sum, chunk_timestamp_list, right_index = True, left_index = True)
    chunk_group.reset_index(inplace= True)
    chunk_group.rename(columns = {"index" : "msno"}, inplace = True)
    
    for index, row in chunk_group.iterrows():
        if row["msno"] not in user_logs_dict_keys:
            user_logs_dict_keys.add(row["msno"])
            user_logs_dict[row["msno"]] = {"num_25_sum" : row["num_25"],
                                           "num_50_sum" : row["num_50"],
                                           "num_75_sum" : row["num_75"],
                                           "num_985_sum" : row["num_985"],
                                           "num_100_sum" : row["num_100"],
                                           "num_unq_sum" : row["num_unq"],
                                           "total_secs_sum" : row["total_secs"],
                                           "date_list": [row["date"]],
                                           "first_date": min(row["date"]),
                                           "last_date": max(row["date"]),
                
                }
        else: 
            print(row["msno"])
            user_logs_dict[row["msno"]]["num_25_sum"] = user_logs_dict[row["msno"]]["num_25_sum"] + row["num_25"]
            user_logs_dict[row["msno"]]["num_50_sum"] = user_logs_dict[row["msno"]]["num_50_sum"] + row["num_50"]
            user_logs_dict[row["msno"]]["num_75_sum"] = user_logs_dict[row["msno"]]["num_75_sum"] + row["num_75"]
            user_logs_dict[row["msno"]]["num_985_sum"] = user_logs_dict[row["msno"]]["num_985_sum"] + row["num_985"]
            user_logs_dict[row["msno"]]["num_100_sum"] = user_logs_dict[row["msno"]]["num_100_sum"] + row["num_100"]
            user_logs_dict[row["msno"]]["num_unq_sum"] = user_logs_dict[row["msno"]]["num_unq_sum"] + row["num_unq"]
            user_logs_dict[row["msno"]]["total_secs_sum"] = user_logs_dict[row["msno"]]["total_secs_sum"] + row["total_secs"]
#            for date_element in row["date"]:
#                user_logs_dict[row["msno"]]["date_list"].extend(date_element)
            user_logs_dict[row["msno"]]["date_list"].append(row["date"])
            date_list_new = []
            for timestamp_list in user_logs_dict[row["msno"]]["date_list"]:
                date_list_new.extend(timestamp_list) 
            user_logs_dict[row["msno"]]["date_list"] = date_list_new
            user_logs_dict[row["msno"]]["first_date"] = min(user_logs_dict[row["msno"]]["date_list"])
            user_logs_dict[row["msno"]]["last_date"] = max(user_logs_dict[row["msno"]]["date_list"])             
#            user_logs_dict[row["msno"]]["nb_records"] = user_logs_dict[row["msno"]]["nb_records"] + 1 
    
     
    cont +=1
    if cont % 100 == 0:
        print ("%d chunks processed in %d seconds" %(cont, time.time() - start_time))                          
        with open(user_logs_dict_path, "wb") as f:
                pickle.dump(user_logs_dict, f)
            
            
                    
            
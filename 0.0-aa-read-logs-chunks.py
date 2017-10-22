#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 11:35:13 2017

@author: andraa
"""

import pandas as pd
from pymongo import MongoClient
import sys
import numpy as np
import pickle
import gc
from datetime import datetime
import time

# script that takes all the data in the user logs and keeps just the users from members list

# function that fill convert the date from 20160607 format into datetime
def convert_date(x):
    x = str(x)
    date = datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8]))
    return date

# read the user logs
user_logs = pd.read_csv("/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/user_logs.csv",
                        chunksize = 100000, iterator = True)

# read list of members that are to be extracted from the database
members_list_path = "/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/members_list.pkl"
with open(members_list_path, 'rb') as f:
    members_list = list(pickle.load(f))
members_df = pd.DataFrame(members_list, columns = ["msno"])

# path where the user logs with just the members from the list will be stored
user_log_merge_path = "/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/user_log_merge/user_log_merge_{:d}.csv"

start_time = time.time()
k=0
for chunk in user_logs:
    if k%100 == 0:
        print("%d chunks were processed in %d seconds" %(k, time.time() - start_time ))
    # keep only relevant members from the chunk
    user_log_merge = pd.merge(chunk, members_df, how = "inner", on = "msno")
    # change the date format
#    user_log_merge["date"] = convert_date(user_log_merge["date"])
    # write csv
    user_log_merge.to_csv(user_log_merge_path.format(k))
    k+=1

print(time.time() - start_time)
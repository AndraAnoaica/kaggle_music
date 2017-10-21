#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 19:37:12 2017

@author: andraa
"""
# get users that are in the training and test set

import pandas as pd
import pickle

# read the data
train_df = pd.read_csv("/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/train.csv")
test_df = pd.read_csv("C/sample_submission_zero.csv")

# create the list
members_list_path = "/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/members_list.pkl"
members_list = set(train_df['msno'].values)
members_list.update(set(test_df['msno'].values))

# save the list in pickle format 
with open(members_list_path, "wb") as f:
    pickle.dump(members_list,f)

    
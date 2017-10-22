#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 14:37:29 2017

@author: andraa
"""

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.metrics import recall_score
from imblearn.over_sampling import SMOTE
from imblearn.over_sampling import RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler

training_df = pd.read_csv("/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/train.csv")

training_df.groupby('is_churn').count()
"""
            msno
is_churn        
0         929460
1          63471
"""


# random under sampling
########################
def undersample(x_train, y_train):
    rus = RandomUnderSampler()
    x_train_res, y_train_res = rus.fit_sample(x_train, y_train)
    return x_train_res, y_train_res

# random over sample
#######################################
def oversample(x_train, y_train):
    ros = RandomOverSampler()
    x_train_res, y_train_res = ros.fit_sample(x_train, y_train)
    return x_train_res, y_train_res

# over sample using the SMOTE algorithm
#######################################
def smote(x_train, y_train):
    sm = SMOTE(random_state=12, ratio = 1.0)
    x_train_res, y_train_res = sm.fit_sample(x_train, y_train)
    return x_train_res, y_train_res
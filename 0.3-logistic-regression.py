#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 15:28:14 2017

@author: andraa
"""

import numpy as np
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.metrics import recall_score



lr = linear_model.LogisticRegression(C=1e5)
lr.fit(x_train_res, y_train_res)

print ('Validation Results')
print (lr.score(x_val, y_val))
print (recall_score(y_val, clf_rf.predict(x_val)))

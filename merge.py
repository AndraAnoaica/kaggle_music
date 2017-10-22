#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 18:13:24 2017

@author: andraa
"""
import datetime
import pandas as pd
from os.path import isfile, join
from os import listdir
  

folder = '/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/user_log_merge'

merge_file = '/media/andraa/10160545101605452/kaggle/WSDM-kaggle/data/intermediate_data/user_log_members.csv'


files = [f for f in listdir(folder) if isfile(join(folder, f))]
f_out = open(merge_file, 'w')
f_init = open(join(folder, files[0]))

print(files[0])
for l in f_init.readlines():
    f_out.write(l)

for f_in in files[1:]:
    print(f_in)
    for l in open(join(folder, f_in)).readlines()[1:]:
        f_out.write(l)


f_out.close()
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:41:23 2020

@author: michael
"""

import os
os.chdir("C:\\Users\\michael\\capstone project")
print("reddit_data.py")
exec(open("reddit_data.py").read())
print("stockdata_etl_halfhour.py")
exec(open("stockdata_etl_halfhour.py").read())
print("dataset_half_hour_merge.py")
exec(open("dataset_half_hour_merge.py").read())
print("svm_model.py")
exec(open("svm_model.py").read())

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 16:41:23 2020

@author: michael
"""

import os
os.chdir("C:\\Users\\michael\\capstone project")
print("reddit_data.py")
exec(open("reddit_data.py").read())
print("StockData_By_Hour_ETL_LAG.py")
exec(open("StockData_By_Hour_ETL_LAG.py").read())
print("Dataset_by_hour_ETL_LAG.py")
exec(open("Dataset_by_hour_ETL_LAG.py").read())
# print("Dataset_by_hour_by_subreddit_ETL_LAG.py")
# exec(open("Dataset_by_hour_by_subreddit_ETL_LAG.py").read())
print("OutlierAnalysis_ETL_lag.py")
exec(open("OutlierAnalysis_ETL_lag.py").read())
# print("OutlierAnalysis_subreddit_ETL.py")
# exec(open("OutlierAnalysis_subreddit_ETL.py").read())
# print("OutlierAnalysis_Control_ETL.py")
# exec(open("OutlierAnalysis_Control_ETL.py").read())
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 14:14:27 2020

@author: michael
"""


import sqlalchemy
import pandas as pd
import os
os.chdir("")
engine = sqlalchemy.create_engine('', pool_recycle=3600)
for symbol in ['^GSPC','^VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
    stock = pd.read_sql("select * from capstone."+symbol.lower().replace("^","")+"_by_hour_ETL_LAG",engine)     
    stock['Next_Delta%'] = ((stock['Close'] - stock['OPEN_current'])/stock['OPEN_current'])*100
    stock['Current_Delta%'] = ((stock['CLOSE_current'] - stock['OPEN_previous'])/stock['OPEN_previous'])*100
    stock['Previous_Delta%'] = ((stock['CLOSE_previous'] - stock['OPEN_previous'])/stock['OPEN_previous'])*100
    stock['Volume_Delta%'] = ((stock['VOLUME_current'] - stock['VOLUME_previous'])/stock['VOLUME_current'])*100


    dataset = pd.DataFrame({'Datetime':stock['Datetime'],'Next_Delta%':stock['Next_Delta%'],'Previous_Delta%':stock['Previous_Delta%'],'Volume_Delta%':stock['Volume_Delta%'],'Current_Delta%':stock['Current_Delta%']})
    dataset = dataset.drop_duplicates()
    dataset.to_csv("none_"+symbol.lower().replace("^","")+"_by_hour_test_data.csv",index = False)















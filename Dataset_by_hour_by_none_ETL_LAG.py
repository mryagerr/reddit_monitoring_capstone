# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 14:14:27 2020

@author: michael
"""


import sqlalchemy
import pandas as pd
import os
os.chdir("C:\\Users\\michael\\capstone project")
engine = sqlalchemy.create_engine('mysql+pymysql://capstone:password@localhost/capstone', pool_recycle=3600)
for symbol in ['^GSPC','^VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
    stock = pd.read_sql("select * from capstone."+symbol.lower().replace("^","")+"_by_hour_ETL_LAG where subreddit = '"+subreddit+"'",engine)   
    stock['Next_Delta%'] = ((stock['CLOSE'] - stock['OPEN_ETL_Lag'])/stock['OPEN_ETL_Lag'])*100
    stock['Current_Delta%'] = ((stock['CLOSE_current'] - stock['OPEN_previous'])/stock['OPEN_previous'])*100
    stock['Previous_Delta%'] = ((stock['CLOSE_previous'] - stock['OPEN_previous'])/stock['OPEN_previous'])*100
    stock['Volume_Delta%'] = ((stock['VOLUME_current'] - stock['VOLUME_previous'])/stock['VOLUME_current'])*100
    stock['gspc_mentioned'] = 0
    stock.loc[stock['gspc'].notnull(),'gspc_mentioned'] = 1
    stock['vix_mentioned'] = 0
    stock.loc[stock['vix'].notnull(),'vix_mentioned'] = 1
    stock['Info_Age(minutes)'] = (stock['Orginal_Datetime'] - stock['Datetime']).dt.total_seconds()/60
    
    dataset = stock[stock['Info_Age(minutes)'] == 0]
    dataset = pd.DataFrame({'Next_Delta%':dataset['Next_Delta%'],'Previous_Delta%':dataset['Previous_Delta%'],'Orginal_Datetime':dataset['Orginal_Datetime']})
    dataset = dataset.drop_duplicates()
    
    values=['Current_Delta%','Previous_Delta%','Volume_Delta%']
    
    dataset_stock_avg = stock.groupby(['Orginal_Datetime'])[values].mean()
    dataset_stock_avg.columns = "mean_"+dataset_stock_avg.columns
    dataset_stock_median = stock.groupby(['Orginal_Datetime'])[values].median()
    dataset_stock_median.columns = "median_"+dataset_stock_median.columns
    dataset_stock_std = stock.groupby(['Orginal_Datetime'])[values].std()
    dataset_stock_std.columns = "std_"+dataset_stock_std.columns
    dataset_stock_10th = stock.groupby(['Orginal_Datetime'])[values].quantile(.1)
    dataset_stock_10th.columns = "10th_"+dataset_stock_10th.columns
    dataset_stock_90th = stock.groupby(['Orginal_Datetime'])[values].quantile(.9)
    dataset_stock_90th.columns = "90th_"+dataset_stock_90th.columns
    dataset = dataset.merge(dataset_stock_avg,on = ['Orginal_Datetime']).merge(dataset_stock_median,on = ['Orginal_Datetime']).merge(dataset_stock_std,on = ['Orginal_Datetime'])
    dataset = dataset.merge(dataset_stock_90th,on = ['Orginal_Datetime']).merge(dataset_stock_10th,on = ['Orginal_Datetime'])
    dataset = dataset.fillna(0)
    dataset.to_csv("none_"+symbol.lower().replace("^","")+"_by_hour_test_data.csv",index = False)















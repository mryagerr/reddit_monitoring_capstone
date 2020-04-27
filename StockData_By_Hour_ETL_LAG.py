# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 21:28:04 2020

@author: michael
"""


import requests
import pandas as pd
import arrow
import datetime
import sqlalchemy
import yfinance as yf 
# Get the data for the stock Apple by specifying the stock ticker, start date, and end date
engine = sqlalchemy.create_engine('', pool_recycle=3600)

def offset(x): return max(x-4,0)

commentdata= pd.read_sql("select * from reddit",engine)
commentdata['created'] = pd.to_datetime(commentdata['created'],unit = 's')
commentdata['comment_Date'] = commentdata['created'].dt.weekday
#Turn time to EDT, created is 8 hours from UTC and UTC is 4 hours from EDT
commentdata['Datetime'] = commentdata['created'] - pd.DateOffset(hours=8)
commentdata['Datetime'] =  commentdata['Datetime'].dt.round('30min')   + pd.DateOffset(hours=1)
commentdata['Word_Count'] = commentdata.comment.str.len()
commentdata.to_sql("comment_data",engine,if_exists = 'replace',index = False)



for symbol in ['^GSPC','^VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
    df = pd.DataFrame(yf.download(symbol, interval = "30m",period = "60d")).reset_index()
    df['Datetime'] = pd.to_datetime(df['Datetime'],utc = True).dt.tz_localize(None)
    #Change from EDT to UTC, 5 hours for daylight sayings time
    df['Datetime'] = df['Datetime'].dt.round('30min')
    datamerge = df.merge(commentdata,on = ['Datetime'],how='inner')
    df['Datetime'] = df['Datetime'] - pd.DateOffset(minutes=60)
    df.columns = ['Datetime', 'OPEN_current', 'HIGH_current', 'LOW_current', 'CLOSE_current','Adj_Close_current', 'VOLUME_current']
    datamerge = datamerge.merge(df,on = ['Datetime'],how='inner')
    df['Datetime'] = df['Datetime'] - pd.DateOffset(minutes=30)
    df.columns = ['Datetime', 'OPEN_previous', 'HIGH_previous', 'LOW_previous', 'CLOSE_previous','Adj_Close_previous', 'VOLUME_previous']
    datamerge = datamerge.merge(df,on = ['Datetime'],how='inner')
    datamerge.to_sql(symbol.lower().replace("^","")+"_by_hour_etl_lag",engine,if_exists = 'replace',index = False)
    print(symbol)

    

















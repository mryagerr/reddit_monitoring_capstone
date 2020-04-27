# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 08:58:57 2020

@author: Michael Petrillo

  
"""



# Imports
import numpy as np
import sqlalchemy
import pandas as pd
from sklearn.model_selection import train_test_split
engine = sqlalchemy.create_engine('', pool_recycle=3600)
import matplotlib.pyplot as plt
from scipy.special import expit as sigmoid
from sklearn.neural_network import MLPRegressor
from sklearn.neural_network import MLPClassifier
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
from sklearn import preprocessing
from scipy.stats import normaltest
from sklearn import svm
MMS = preprocessing.MinMaxScaler()
sc =  preprocessing.StandardScaler()
rs = preprocessing.RobustScaler()
for subreddit in ['ALL','NONE',"WallStreetBets","stocks","Investing","StockMarket"]:
    for symbol in ['^GSPC','^VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
        if subreddit == 'ALL':
            dataset = pd.read_csv(symbol.lower().replace("^","")+"_by_hour_test_data.csv")
        elif subreddit == 'NONE':
            dataset = pd.read_csv("none_"+symbol.lower().replace("^","")+"_by_hour_test_data.csv")
        else:
            dataset = pd.read_csv(subreddit+"_"+symbol.lower().replace("^","")+"_by_hour_test_data.csv")
        
        # Each row is a training example, each column is a feature  [X1, X2, X3]
        target_value = 'Next_Delta%'
        values = dataset.columns.values.tolist()
        values.remove(target_value)
        values.remove('Datetime')
        dataset.replace([np.inf, -np.inf], np.nan, inplace=True)
        dataset = dataset.fillna(0)
        # dataset = dataset.reindex()
        for columncheck in values:
            if normaltest(dataset[columncheck])[1] > 0.1:
                    print("normal-"+columncheck)
                    dataset[columncheck] = sc.fit_transform(dataset[[columncheck]].values.astype(float))
            else:
                    print("power-"+columncheck)
                    dataset[columncheck] = preprocessing.PowerTransformer(method='yeo-johnson').fit_transform(dataset[[columncheck]].values.astype(float))
            # else:
                # dataset[columncheck] = sigmoid(dataset[columncheck])
        dataset[target_value] = ((dataset[target_value]*2).round(0)/2)*10
        
        y=dataset[target_value].astype("float").values
        X=dataset[values].astype("float").values
        
    
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=1,shuffle = True)

        clf = svm.SVC()
        clf.fit(X_train, y_train.astype(int))
        pd.DataFrame({'Score':[clf.score(X_test, y_test.astype(int))],'Subreddit':[subreddit],'Symbol':[symbol]}).to_sql("svm_results_etl_gran",engine,if_exists = 'append',index = False)

        
        

        df = pd.DataFrame({'Subreddit':subreddit,'Symbol':symbol,'Y':y_train.astype(int),'X':clf.predict(X_train)}).to_sql("svm_results_etl_linear_results_granular",engine,if_exists = 'append',index = False)
        

            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        


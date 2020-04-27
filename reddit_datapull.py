# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 15:46:14 2020

@author: michael
"""


import praw
import pandas as pd
from textblob import TextBlob
import numpy as np
import sqlalchemy

engine = sqlalchemy.create_engine('', pool_recycle=3600)
reddit = praw.Reddit(client_id='', \
                     client_secret='', \
                     user_agent='WebScrap', \
                     username='', \
                     password='')
    
# ['WallStreetBets','SecurityAnalysis','StockADay','Investing','RobinHood','StockMarket','stocks','Daytrading']
    
doublecheck = pd.read_sql("select id,subreddit,type from capstone.reddit",engine)    
#(1577836800,1586034261)
for subredditchoosen in  ['StockADay','Investing','RobinHood','StockMarket','stocks','Daytrading','WallStreetBets','SecurityAnalysis',]:
    for submission in reddit.subreddit(subredditchoosen).top('week'):
     if submission.id not in doublecheck.id.to_list():
            break
            response = pd.DataFrame({"id":[submission.id],
                                            "comment":[submission.title],
                                             "score":[submission.score],
                                             "author":[str(submission.author)],
                                             "created":[submission.created],
                                             "replies_count":[len(submission.comments.list())]})
            response['subreddit'] = subredditchoosen
            response['type'] = 'title'
            response['polarity'],response['subjectivity'] = TextBlob(submission.title).sentiment[0],TextBlob(submission.title).sentiment[1]
            list_of_words = submission.title.lower().split()
            for stocksearch_place in ['GSPC','VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
                response[stocksearch_place.lower()] = np.nan
            for stocksearch in [['nvidia','nvda'],['nvda','nvda'],['aws','amzn'],['amzn','amzn'],['amazon','amzn'],['vix','vix'],['disney','dis'],['dis','dis'],['spy','gspc'],['s&p 500','gspc'],['s&p','gspc'],['aapl','aapl'],['apple','aapl'],['tesla','tlsa'],['tlsa','tlsa'],['netflix','nflx'],['nflx','nflx'],['boeing','ba'],['ba','ba'],['wmt','wmt'],['wallmart','wmt']]:
                if stocksearch[0] in list_of_words:
                    if list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "put" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "put":
                                        response[stocksearch[1]] = -1
                    elif list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "call" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "call":
                        response[stocksearch[1]] = 1
                    else: 
                        response[stocksearch[1]] = 0
            response['movement'] = np.nan
            if ("call" in list_of_words or "buy" in list_of_words) and ("put" in list_of_words or "sell" in list_of_words):
                response['movement'] = 0
            elif "call" in list_of_words or "buy" in list_of_words:
                response['movement'] = 1
            elif "put" in list_of_words or "sell" in list_of_words:
                response['movement'] = -1
            response.to_sql("reddit",engine,if_exists = 'append',index = False)
            for comments in list(submission.comments):
                if 'body' in dir(comments):
                    if comments.id not in doublecheck.id.to_list():
                        # print(comments.body)
                        response = pd.DataFrame({"id":[comments.id],
                            "comment":[comments.body],
                                                 "score":[comments.score],
                                                 "author":[str(comments.author)],
                                                 "created":[comments.created],
                                                 "replies_count":[len(comments.replies.list())]})
                        response['subreddit'] = subredditchoosen
                        response['type'] = 'comment'
                        response['polarity'],response['subjectivity'] = TextBlob(comments.body).sentiment[0],TextBlob(comments.body).sentiment[1]
                        list_of_words = comments.body.lower().split()
                        for stocksearch_place in ['GSPC','VIX','AAPL','DIS','TLSA','NFLX','BA','WMT','AMZN','NVDA']:
                            response[stocksearch_place.lower()] = np.nan
                        for stocksearch in [['nvidia','nvda'],['nvda','nvda'],['amzn','amzn'],['amazon','amzn'],['vix','vix'],['disney','dis'],['dis','dis'],['spy','gspc'],['s&p 500','gspc'],['s&p','gspc'],['aapl','aapl'],['apple','aapl'],['tesla','tlsa'],['tlsa','tlsa'],['netflix','nflx'],['nflx','nflx'],['boeing','ba'],['ba','ba'],['wmt','wmt'],['wallmart','wmt']]:
                            if stocksearch[0] in list_of_words:
                                if list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "put" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "put":
                                                    response[stocksearch[1]] = -1
                                elif list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "call" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "call":
                                    response[stocksearch[1]] = 1
                                else: 
                                    response[stocksearch[1]] = 0
                        response['movement'] = np.nan
                        if ("call" in list_of_words or "buy" in list_of_words) and ("put" in list_of_words or "sell" in list_of_words):
                            response['movement'] = 0
                        elif "call" in list_of_words or "buy" in list_of_words:
                            response['movement'] = 1
                        elif "put" in list_of_words or "sell" in list_of_words:
                            response['movement'] = -1
                        response.to_sql("reddit",engine,if_exists = 'append',index = False)
                if "replies" in dir(comments):
                 if len(comments.replies.list()) > 0:
                    for replies in comments.replies.list():
                        if 'body' in dir(replies):
                            if replies.id not in doublecheck.id.to_list():
                                    # print(replies.body)
                                    response = pd.DataFrame({"id":[replies.id],
                                        "comment":[replies.body],
                                                     "score":[replies.score],
                                                     "author":[str(replies.author)],
                                                     "created":[replies.created],
                                                     "replies_count":[len(replies.replies.list())]})
                                    response['subreddit'] = subredditchoosen
                                    response['type'] = 'response'
                                    response['polarity'],response['subjectivity'] = TextBlob(replies.body).sentiment[0],TextBlob(replies.body).sentiment[1]
                                    list_of_words = replies.body.lower().split()
                                    for stocksearch_place in ['GSPC','VIX','AAPL','DIS','TLSA','NFLX','BA','WMT']:
                                        response[stocksearch_place.lower()] = np.nan
                                    for stocksearch in [['disney','dis'],['dis','dis'],['spy','gspc'],['s&p 500','gspc'],['s&p','gspc'],['aapl','aapl'],['apple','aapl'],['tesla','tlsa'],['tlsa','tlsa'],['netflix','nflx'],['nflx','nflx'],['boeing','ba'],['ba','ba'],['wmt','wmt'],['wallmart','wmt']]:
                                        if stocksearch[0] in list_of_words:
                                            if list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "put" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "put":
                                                response[stocksearch[1]] = -1
                                            elif list_of_words[max(list_of_words.index(stocksearch[0]) - 1,0)] == "call" or list_of_words[min(list_of_words.index(stocksearch[0]) + 1,len(list_of_words)-1)] == "call":
                                                response[stocksearch[1]] = 1
                                            else: 
                                                response[stocksearch[1]] = 0
                                    response['movement'] = np.nan
                                    if ("call" in list_of_words or "buy" in list_of_words) and ("put" in list_of_words or "sell" in list_of_words):
                                        response['movement'] = 0
                                    elif "call" in list_of_words or "buy" in list_of_words:
                                        response['movement'] = 1
                                    elif "put" in list_of_words or "sell" in list_of_words:
                                        response['movement'] = -1
                                    response.to_sql("reddit",engine,if_exists = 'append',index = False)

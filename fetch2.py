# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 19:01:22 2020

@author: Sushrutha Reddy
"""

import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules
from collections import Counter, OrderedDict
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from sqlalchemy import create_engine
import os
import pickle
import calendar
from datetime import datetime
from numpy.random import randint
from sqlalchemy import create_engine
import calendar 
# ------------------------------------------------------------------------------
# input of the required month
#no_of_months = 5
#year =2020


def rules_fetch(no_of_months,year):
    global month

    def frozentolist(fset: 'frozenset'):
        assert isinstance(fset, frozenset)
        ifset = iter(fset)
        while True:
            try:
                yield next(ifset)
            except StopIteration:
                break

    def to_string(ls):  
        ls = list(ls)
        return ",".join(ls)

    ####################### Data Collection ##########################################
    # Connection betwee sql and python
    connection = create_engine("mysql+pymysql://root:Sushru@123@localhost/agro_mba")
    # Creating Dataframe from MySql DB table
    Cmd = "select * from mba_data"
    mba_data = pd.read_sql(Cmd, connection, index_col = "index")

    #mba_data = pd.read_excel(os.getcwd() + "/Order_Data (2).xlsx", encoding='unicode_escape')
    # data cleaning
    def clean(df):
        df['order_fruits'].fillna((df['order_fruits'].mode()[0]), inplace=True)
        df['order_vegetables'].fillna((df['order_vegetables'].mode()[0]), inplace=True)
        df['order_rice'].fillna((df['order_rice'].mode()[0]), inplace=True)
        df['order_milk'].fillna((df['order_milk'].mode()[0]), inplace=True)
        df['basket'].fillna((df['basket'].mode()[0]), inplace=True)
        
        return ()

    clean(mba_data)
    mba_data.isnull().sum()
    # -----------------------------------------------------------------------------#
    ############################## EDA ON COMPLETE DATASET ################################

    #####Top 10 associated transactions of fruits,vegetables,rice and milk bought ######
    mba_data['order_fruits'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    mba_data['order_vegetables'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    mba_data['order_milk'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    mba_data['order_rice'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')

    

   # Frequency meaasurement -Top 10 Fruit products --
    fruit_df = (mba_data.assign(category = mba_data['order_fruits'].str.split(',')).explode('category').reset_index(drop=True))
    fruit_df.head(10)

    fig, ax=plt.subplots(figsize=(6,4))
    fruit_df['category'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Frequency of Fruits orders per quantities')
    plt.xlabel('Fruits')



# Frequency meaasurement -Top 10 Vegetable products --
    veg_df = (mba_data.assign(category = mba_data['order_vegetables'].str.split(',')).explode('category').reset_index(drop=True))
    veg_df.head(10)

    fig, ax=plt.subplots(figsize=(6,4))
    veg_df['category'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Frequency of Vegetable orders per quantities')
    plt.xlabel('Vegetables')



# Frequency meaasurement -Top 10 Rice products --
    rice_df = (mba_data.assign(category = mba_data['order_rice'].str.split(',')).explode('category').reset_index(drop=True))
    rice_df.head(10)

    fig, ax=plt.subplots(figsize=(6,4))
    rice_df['category'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Frequency of Rice orders per quantities')
    plt.xlabel('Rice type')


# Frequency meaasurement -Top 10 Milk products --
    milk_df = (mba_data.assign(category = mba_data['order_milk'].str.split(',')).explode('category').reset_index(drop=True))
    milk_df.head(10)

    fig, ax=plt.subplots(figsize=(6,4))
    milk_df['category'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Frequency of Milk orders per quantities')
    plt.xlabel('Milk type')



# Frequency meaasurement -Top 10 basket products --
    basket_df = (mba_data.assign(category = mba_data['basket'].str.split(',')).explode('category').reset_index(drop=True))
    basket_df.head(10)

    fig, ax=plt.subplots(figsize=(6,4))
    basket_df['category'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Frequency')
    plt.xlabel('Items')


#### To find the number of unique items added in the dataframe
    mba_data['basket'].nunique()

##Top 10 transactions from the basket#####
    fig, ax=plt.subplots(figsize=(6,4))
    mba_data['basket'].value_counts().sort_values(ascending=False).head(10).plot(kind='bar')
    plt.ylabel('Number of transactions')
    plt.xlabel('Items')

#datewise number of transactions----
############ADDITION OF COLUMNS FOR CUSTOMER ANALYSIS ALONGWITH MONTHS AND COLUMNS -----
##First ascending for the number of transaction of each date and month number of each year ####
    mba_data.groupby('order_date')['basket'].count().sort_values(ascending=False).head(50)
    mba_data['order_date']=pd.to_datetime(mba_data['order_date'])
    mba_data['Day']=mba_data['order_date'].dt.day_name()
    mba_data['Month_number']=mba_data['order_date'].dt.month
    mba_data['Month']=mba_data['order_date'].dt.month_name()
    mba_data['Year']=mba_data['order_date'].dt.year
    mba_data['Year_Month']=mba_data['Year'].apply(str)+' '+mba_data['Month'].apply(str)
#mba_data.index.name='order_date'
#mba_data.head()

## Reviewing the changes made with the name of months&years with frequent sales -----

    mba_data.groupby('Year_Month')['basket'].count().plot(kind='bar')
    plt.ylabel('Number of transactions')
    plt.title('Business during the past months')

## TO FIND OUT MAXIMUM SALE OF PRODUCTS AND THE MOST SALE OF PRODUCTS IN SPECIFIED MONTHS----
    mba_data.groupby('Month')['basket'].count().plot(kind='bar')

## TO FIND OUT MAXIMUM SALE OF PRODUCTS AND THE MOST SALE OF PRODUCTS IN SPECIFIED DAY----
    mba_data.groupby('Day')['basket'].count().plot(kind='bar')

    #################ITEMS_FREQUENCY ON MONTHLY DATA #############
    mba_data['order_date'] = pd.to_datetime(mba_data['order_date'], format='%d%b%Y')
    mba_data = mba_data.loc[mba_data['order_date'].dt.year == year]
    for month in range(1, no_of_months + 1):
        print(month)
        month_mba_data = mba_data.loc[mba_data['order_date'].dt.month == month]
    
    ######################## FRUITS & VEGETABLES FREQUENCY ##########################
    
      ############################### FRUITS FREQUENCY ##########################
    
    mba_data_F = month_mba_data['order_fruits'].values.tolist()
    mba_data_F = [x for x in mba_data_F if x != None]

    mba_data_list_F = []
    for i in mba_data_F:mba_data_list_F.append(i.split(","))

    all_mba_data_list_F = [i for item in mba_data_list_F for i in item]
    all_mba_data_list_F = [line for line in all_mba_data_list_F if line.strip() != ""]

    item_frequencies_F = Counter(all_mba_data_list_F)
    item_frequencies_F = sorted(item_frequencies_F.items(),key = lambda x:x[1])

    # Storing frequencies and items in separate variables 
    COUNT_FREQUENCY_F = list(reversed([i[1] for i in item_frequencies_F]))
    FRUITS = list(reversed([i[0] for i in item_frequencies_F]))

    mba_data_fruits  = pd.DataFrame(pd.Series(FRUITS))
    mba_data_fruits.columns = ["FRUITS"]
    mba_data_fruits.insert(1, "COUNT_FREQUENCY", list(reversed([i[1] for i in item_frequencies_F]))) 
    mba_data_fruits.insert(0,"YEAR",year) 
    mba_data_fruits["MONTH_NUMBER"] = [month for i in range(mba_data_fruits.shape[0])]
    sec_col = mba_data_fruits.pop('MONTH_NUMBER')
    mba_data_fruits.insert(1, "MONTH_NUMBER", sec_col)
    
    Top_3_F_itemsets = mba_data_fruits.head(3)

      ########################## VEGETABLES FREQUENCY ######################

    mba_data_V = month_mba_data['order_vegetables'].values.tolist()
    mba_data_V = [x for x in mba_data_V if x != None]

    mba_data_list_V = []
    for i in mba_data_V:mba_data_list_V.append(i.split(","))

    all_mba_data_list_V = [i for item in mba_data_list_V for i in item]
    all_mba_data_list_V = [line for line in all_mba_data_list_V if line.strip() != ""]

    item_frequencies_V = Counter(all_mba_data_list_V)
    item_frequencies_V = sorted(item_frequencies_V.items(),key = lambda x:x[1])
    
    # Storing frequencies and items in separate variables 
    COUNT_FREQUENCY_V = list(reversed([i[1] for i in item_frequencies_V]))
    VEGETABLES = list(reversed([i[0] for i in item_frequencies_V]))


    mba_data_vegetables = pd.DataFrame(pd.Series(VEGETABLES))
    mba_data_vegetables.columns = ["VEGETABLES"]
    mba_data_vegetables.insert(1, "COUNT_FREQUENCY_V", list(reversed([i[1] for i in item_frequencies_V]))) 
    Top_3_V_itemsets = mba_data_vegetables.head(3)

    ### Merging Fruits and Vegetables frequency df's
    monthly_frequency_Fruits_Vegetables=Top_3_F_itemsets.reset_index(drop=True).merge(Top_3_V_itemsets.reset_index(drop=True), left_index=True, right_index=True)
    # Defining month name into monthly_frequency_Fruits_Vegetables df
    monthly_frequency_Fruits_Vegetables['MONTH'] = monthly_frequency_Fruits_Vegetables['MONTH_NUMBER'].apply(lambda x: calendar.month_name[x])
    third_col = monthly_frequency_Fruits_Vegetables.pop('MONTH')
    monthly_frequency_Fruits_Vegetables.insert(2, "MONTH", third_col)
    
    # Storing into sql database
    monthly_frequency_Fruits_Vegetables.to_sql('Fruits_Vegetables_monthly_frequency', connection,index=False, if_exists='append')

    #---------------------------------------------------------------------------------
    
    ########################### RICE & MILK FREQUENCY #####################
    
      #######################   RICE FREQUENCY   #####################

    mba_data_R = month_mba_data['order_rice'].values.tolist()
    mba_data_R = [x for x in mba_data_R if x != None]

    mba_data_list_R = []
    for i in mba_data_R:mba_data_list_R.append(i.split(","))

    all_mba_data_list_R = [i for item in mba_data_list_R for i in item]
    all_mba_data_list_R = [line for line in all_mba_data_list_R if line.strip() != ""]

    item_frequencies_R = Counter(all_mba_data_list_R)
    item_frequencies_R = sorted(item_frequencies_R.items(),key = lambda x:x[1])

    # Storing frequencies and items in separate variables 
    COUNT_FREQUENCY_R = list(reversed([i[1] for i in item_frequencies_R]))
    RICE = list(reversed([i[0] for i in item_frequencies_R]))

    mba_data_rice  = pd.DataFrame(pd.Series(RICE))
    mba_data_rice.columns = ["RICE"]
    mba_data_rice.insert(1, "COUNT_FREQUENCY_R", list(reversed([i[1] for i in item_frequencies_R]))) 
    mba_data_rice.insert(0,"YEAR",year) 
    mba_data_rice["MONTH_NUMBER"] = [month for i in range(mba_data_rice.shape[0])]
    sec_col = mba_data_rice.pop('MONTH_NUMBER')
    mba_data_rice.insert(1, "MONTH_NUMBER", sec_col)

    Top_3_R_itemsets = mba_data_rice.head(3)


      ######################### MILK FREQUENCY ########################

    mba_data_M = month_mba_data['order_milk'].values.tolist()
    mba_data_M = [x for x in mba_data_M if x != None]

    mba_data_list_M = []
    for i in mba_data_M:mba_data_list_M.append(i.split(","))

    all_mba_data_list_M = [i for item in mba_data_list_M for i in item]
    all_mba_data_list_M = [line for line in all_mba_data_list_M if line.strip() != ""]

    item_frequencies_M = Counter(all_mba_data_list_M)
    item_frequencies_M = sorted(item_frequencies_M.items(),key = lambda x:x[1])

    # Storing frequencies and items in separate variables 
    COUNT_FREQUENCY_M = list(reversed([i[1] for i in item_frequencies_M]))
    MILK = list(reversed([i[0] for i in item_frequencies_M]))

    mba_data_Milk = pd.DataFrame(pd.Series(MILK))
    mba_data_Milk.columns = ["MILK"]
    mba_data_Milk.insert(1, "COUNT_FREQUENCY_M", list(reversed([i[1] for i in item_frequencies_M]))) 
    Top_3_M_itemsets = mba_data_Milk.head(3)

    ### Merging Rice and Milk frequency df's
    monthly_frequency_Rice_Milk=Top_3_R_itemsets.reset_index(drop=True).merge(Top_3_M_itemsets.reset_index(drop=True), left_index=True, right_index=True)
    # Defining month name into monthly_frequency_Rice_Milk df
    monthly_frequency_Rice_Milk['MONTH'] = monthly_frequency_Rice_Milk['MONTH_NUMBER'].apply(lambda x: calendar.month_name[x])
    third_col = monthly_frequency_Rice_Milk.pop('MONTH')
    monthly_frequency_Rice_Milk.insert(2, "MONTH", third_col)
    
    # Storing into sql database
    monthly_frequency_Rice_Milk.to_sql('Rice_Milk_monthly_frequency', connection,index=False, if_exists='append')
    
    #-----------------------------------------------------------------------------------
    
    ########################## MODEL BUILDING ##########################
    ################### TOP 100 ASSOCIATION RULES ON MONTHLY DATA ########
    ##########Apriori Algorithm OR Association Rule ######-----
    
    month_mba_data.columns # Display all column names
    month_mba_data.head() # Display few dataset
    month_mba_data.shape # Shape of the data
    month_mba_data.isnull().sum(axis=0) # Checking if missing values and missing values are not found
    month_mba_data.shape #Shape of the data after removing the missing values
    month_mba_data.info() # Information of data
    month_mba_data.describe()
	
    # Converting the month_mba_data dataframe into list format
    month_mba_data = month_mba_data['basket'].values.tolist()
    month_mba_data = [x for x in month_mba_data if x != None]
    mba_data_list = []
    for i in month_mba_data:
        mba_data_list.append(i.split(","))
    
    all_mba_data_list = [i for item in mba_data_list for i in item]
    all_mba_data_list = [line for line in all_mba_data_list if line.strip() != ""]
    item_frequencies = Counter(all_mba_data_list)
    # after sorting
    item_frequencies = sorted(item_frequencies.items(),key = lambda x:x[1])

    # Storing frequencies and items in separate variables
    frequencies = list(reversed([i[1] for i in item_frequencies]))
    items = list(reversed([i[0] for i in item_frequencies]))
	
    #barplot of top 10 given month products
    plt.bar(x=items[0:10],height = frequencies[0:10],color='rgbkymc');
    plt.xticks(rotation=90);
    plt.xlabel("items")
    plt.ylabel("Count")
    plt.show()
    # Creating Data Frame for the transactions data
    mba_data_series = pd.DataFrame(pd.Series(mba_data_list))
    mba_data_series.columns = ["transactions"]
    # creating a dummy columns for the each item in each transactions 
    X = mba_data_series['transactions'].str.join(sep='*').str.get_dummies(sep='*')
    frequent_itemsets = apriori(X, min_support=0.05, max_len=5, use_colnames=True)
    
    # Most Frequent item sets based on support
    frequent_itemsets.sort_values('support',ascending = False,inplace=True)
    #barplot based on support for top 10 products
    plt.bar(x=items[0:10],height = frequent_itemsets.support[0:10],color='rgbkymc');
    plt.xticks(rotation=90);
    plt.xlabel("item-sets")
    plt.ylabel("support")
    plt.show()
    
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold=1)
    rules.head(20)
    rules.sort_values('lift', ascending=False).head(10)

    def to_list(i):
	      return (sorted(list(i)))
    ma_X = rules.antecedents.apply(to_list)+rules.consequents.apply(to_list)
    ma_X1 = ma_X.apply(sorted)
    rules_sets = list(ma_X)
    unique_rules_sets = [list(m) for m in set(tuple(i) for i in rules_sets)]
    index_rules = []
    for i in unique_rules_sets:
	      index_rules.append(rules_sets.index(i))
    # getting rules without any redudancy
    rules_no_redudancy = rules.iloc[index_rules,:]
    # Sorting them with respect to list and getting top 100 rules
    rules_no_redudancy=rules_no_redudancy.sort_values('lift',ascending=False).head(100)
    rules_no_redudancy["antecedents"] = rules_no_redudancy["antecedents"].apply(frozentolist)
    rules_no_redudancy["antecedents"]=rules_no_redudancy["antecedents"].apply(to_string)
    rules_no_redudancy["consequents"] = rules_no_redudancy["consequents"].apply(frozentolist)
    rules_no_redudancy["consequents"] = rules_no_redudancy["consequents"].apply(to_string)
    rules_no_redudancy["RANK"] = np.arange(1, rules_no_redudancy.shape[0]+1)


    rules_no_redudancy.insert(0, 'RULES GENERATED ON', pd.datetime.now().replace(microsecond=0))
    first_col = rules_no_redudancy.pop('RANK')
    rules_no_redudancy.insert(1, "RANK", first_col)

    rules_no_redudancy["MONTH_NUMBER"] = [month for i in range(rules_no_redudancy.shape[0])]
    rules_no_redudancy.insert(2,"YEAR",year) 
    rules_no_redudancy['MONTH'] = rules_no_redudancy['MONTH_NUMBER'].apply(lambda x: calendar.month_name[x])

    third_col = rules_no_redudancy.pop('MONTH_NUMBER')
    rules_no_redudancy.insert(3, "MONTH_NUMBER", third_col)

    fourth_col = rules_no_redudancy.pop('MONTH')
    rules_no_redudancy.insert(4, "MONTH", fourth_col)
    
    seven_col = rules_no_redudancy.pop('support')
    rules_no_redudancy.insert(7, "support", seven_col)
    eight_col = rules_no_redudancy.pop('confidence')
    rules_no_redudancy.insert(8, "confidence", eight_col)
    nine_col = rules_no_redudancy.pop('lift')
    rules_no_redudancy.insert(9, "lift", nine_col)
    
    rules_no_redudancy=rules_no_redudancy.drop(['antecedent support','consequent support','conviction','leverage'],axis=1)
    rules_no_redudancy=rules_no_redudancy.rename(columns = {'antecedents': 'PEOPLE WHO BOUGHT','consequents': 'ALSO BOUGHT','support': 'ASSOCIATION %','confidence' : 'ASSOCIATION PROBABILITY','lift':'ASSOCIATION/ NO ASSOCIATION RATIO'}, inplace = False)

    rules_no_redudancy.to_sql('monthly_rules', connection,index=False, if_exists='append')

    df1 = rules_no_redudancy.copy()
    #df1.drop(["MONTH_NUMBER"], axis=1, inplace=True)
    df1 = df1[["RANK","RULES GENERATED ON","YEAR","MONTH",'PEOPLE WHO BOUGHT', 'ALSO BOUGHT', 'ASSOCIATION %','ASSOCIATION PROBABILITY','ASSOCIATION/ NO ASSOCIATION RATIO']]
    df1.set_index('RANK', inplace=True)
    df1.index.name=None
    df1 = df1.iloc[0:20]
    return(df1)

#df7 = rules_fetch(5,2020)
#df1.columns()

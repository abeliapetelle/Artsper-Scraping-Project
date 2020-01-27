#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 26 14:49:46 2020

@author: abeliapetelle
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
from sqlalchemy import create_engine

def get_Art_Url(): 
    """" Fonction to get the url of all masterpieces in the website Artsper """
    total_listlinks =[]
    for k in range(74):
        url = (f'https://www.artsper.com/fr/oeuvres-d-art-contemporain?selection=masterpieces&sort=5&page={1+k}')
        html = requests.get(url).content
        soup = BeautifulSoup(html, 'lxml')
        total_listlinks.append(['https://www.artsper.com'+i.get('href') for i in soup.select('#catalog figure>a')])
    return sum(total_listlinks, []) #to have an only list and not a list of list

def get_Dtc_Total_Art(total_listlinks):
    json=[]
    for k in range(len(total_listlinks)):
        dct=dict()
        html = requests.get(total_listlinks[k]).content
        soup = BeautifulSoup(html, 'lxml')
        dct['Artist']=[i.text for i in soup.select('.primary-title')][0]
        dct['Name & Date'] = [i.text.strip('\n                    ') for i in soup.select('h1 .secondary-title')][0]
        dct['Category'] = [i.text for i in soup.select('a .category')][0]
        dct['Price'] = '0' if len(soup.select('#sticky p.media-price'))==0  else soup.select('#sticky p.media-price')[0].text
        dct['Theme'] = [i.text.strip('\n').replace('\n','') for i in soup.select('p.pull-right:nth-child(2)')][4]
        dct['Dimension'] = [i.text for i in soup.select('p.pull-right .measure:first-child')][0]
        dct['City Galery'] = [i.text.strip('\n ') for i in soup.select('.city')][0]
        json.append(dct)    
    return json #the dictionnary

def json_to_SQL(json):
    artsper = pd.DataFrame(json)
    username='Abelia'
    password='password'
    host='localhost'
    database_name='ARTSPER'
    engine=create_engine(f"""mysql+pymysql://{username}:{password}@{host}/{database_name}""")
    artsper.to_sql('data1', engine, index=False, if_exists='replace')
    
if __name__=='__main__':
    data = get_Art_Url()
    json = get_Dtc_Total_Art(data)
    json_to_SQL(json)
 


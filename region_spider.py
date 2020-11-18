import scrapy
import datetime
import json
import os
import pandas as pd
from datetime import date, timedelta

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, 'region_ranking')

sdate = date(2015, 10, 1)   # start date
edate = date(2020, 12, 31)   # end date
delta = edate - sdate       # as timedelta

urls = []

for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    dia = day.day
    mes = datetime.date(1900, day.month, 1).strftime('%B').lower()
    ano = day.year

    urls.append(f'https://www.hltv.org/ranking/teams/{ano}/{mes}/{dia}')

regions = ['North%20America',
           'Europe',
           'South%20America',
           'CIS',
           'Oceania',
           'Asia']


region_url = []


for url in urls:
    [region_url.append(f'{url}/country/{region}') for region in regions]

class RegionSpider(scrapy.Spider):
    name = "region"

    start_urls = region_url

    def parse(self, response):

        region_dict = {
           'North%20America': 'NA',
           'Europe': 'EU',
           'South%20America': 'SA',
           'CIS': 'CIS',
           'Oceania': 'OCEANIA',
           'Asia': 'ASIA'
        }        

        row_list = []
        
        for ranking in response.css('.ranked-team .ranking-header'):
            
            row = []

            row.append(ranking.css('.position::text').get())
            row.append(ranking.css('.points::text').get())
            row.append(str(ranking.css('.rankingNicknames span::text').getall()).lower())
            row_list.append(row)

        
        d = response.url.split('/')[-3]
        m = response.url.split('/')[-4]
        y = response.url.split('/')[-5]

        region = region_dict.get(str(response.url.split('/')[-1]))
        
        cols = ['position', 'points', 'players']
        df = pd.DataFrame(row_list, columns=cols)
        df['date'] = f'{d}-{m}-{y}'
        df['region'] = region

        df.to_csv(os.path.join(DATA_DIR, f'{d}-{m}-{y}-{region}.csv'), index=False)

import scrapy
import datetime
import json
import os
import pandas as pd
from datetime import date, timedelta

BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, 'abs_ranking')

sdate = date(2015, 1, 1)   # start date
edate = date(2020, 12, 31)   # end date
delta = edate - sdate       # as timedelta

url = []

for i in range(delta.days + 1):
    day = sdate + timedelta(days=i)
    dia = day.day
    mes = datetime.date(1900, day.month, 1).strftime('%B').lower()
    ano = day.year

    url.append(f'https://www.hltv.org/ranking/teams/{ano}/{mes}/{dia}')

class HltvSpider(scrapy.Spider):
    name = "hltv"
    start_urls = url

    def parse(self, response):
        
        row_list = []

        for ranking in response.css('.ranked-team .ranking-header'):
            
            row = []

            row.append(ranking.css('.position::text').get())
            row.append(ranking.css('.points::text').get())
            row.append(str(ranking.css('.rankingNicknames span::text').getall()).lower())

            row_list.append(row)

                
        d = response.url.split('/')[-1]
        m = response.url.split('/')[-2]
        y = response.url.split('/')[-3]
        
        cols = ['position', 'points', 'players']
        df = pd.DataFrame(row_list, columns=cols)
        df['date'] = f'{d}-{m}-{y}'

        df.to_csv(os.path.join(DATA_DIR, f'{d}-{m}-{y}.csv'), index=False)

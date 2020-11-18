import scrapy
import os
import re
import pandas as pd
from sqlalchemy import create_engine
import sqlite3


class MatchSpider(scrapy.Spider):
    name = "matches"

    BASE_DIR = os.getcwd()
    URL_DIR = os.path.join(BASE_DIR, 'url_dir')
    DB_PATH = os.path.join(BASE_DIR, 'hltvdb.db')
    CSV_PATH = os.path.join(BASE_DIR, 'matches.csv')

    url_list = []

    matches_df = pd.read_csv(CSV_PATH, encoding='utf-8')

    with open (os.path.join(URL_DIR, 'url.txt'), 'r') as f:
        lines = f.readlines()
        for line in lines:
            url = f'https://www.hltv.org/{line}'.replace('\n', '')
            url_list.append(url)

    for url in url_list:
        if url.split('/')[5] in [str(x) for x in matches_df['2345269']]:

            url_list.remove(url)

    start_urls = url_list

    

    def parse(self, response):

        def get_date(response):

            try:
                date_str = response.css('div.date').extract()
                data_unix = re.findall(r'data-unix="(.*?)"', date_str[0])
            except:
                return None

            return data_unix

        def get_match_results(response):

            try:
                match_result = response.css('div.team div>div::text').extract()
            except:
                match_result = None

            return match_result

        def get_teams(response):
            
            try:
                teams = response.css('div.teamName::text').extract()
            except:
                teams = None

            return teams
        
        def get_performance_id(response):
            
            selector_string = response.xpath("//div[@class='small-padding stats-detailed-stats']")[0].extract()

            try:
                if 'mapstatsid' not in selector_string:

                    performance_id = re.findall(r'"(.*?)"', selector_string)[1].split('/')[3]

                elif 'mapstatsid' in selector_string:

                    performance_id = re.findall(r'"(.*?)"', selector_string)[1].split('/')[4]
            except:

                performance_id = None

            return performance_id

        def get_match_id(response):
            try:
                match_id = response.url.split('/')[-2]
            except:
                match_id = None

            return match_id
        
        def data_to_db(data, engine, table):
            '''Realiza o INSERT de dados conferindo se o ID já existe'''
            ids = data['match_id']
            try:
                engine.execute(f"DELETE FROM {table} AS t1 WHERE t1.match_id in ({ids});")
            except:
                pass
            data.to_sql(table, engine, if_exists="append", index=False)
            return None

        date_unix = get_date(response)
        results = get_match_results(response)
        teams = get_teams(response)
        performance_id = get_performance_id(response)
        match_id = get_match_id(response)

        row = []

        row.append(int(match_id))
        row.append(int(performance_id))
        [row.append(int(element)) for element in list(date_unix)]
        [row.append(int(element)) for element in list(results)]
        [row.append(str(element)) for element in list(teams)]


        cols = ['match_id', 
                'performance_id', 
                'date_unix', 
                'team1_score', 
                'team2_score', 
                'first_team', 
                'second_team']

        row_df = pd.DataFrame(columns=cols)
        row_series = pd.Series(row, index=row_df.columns)
        row_df = row_df.append(row_series, ignore_index=True)
        row_df = row_df[cols]

        if 'matches.csv' not in BASE_DIR:
            row_df.to_csv(CSV_PATH, mode='a', header=False)
        else:
            row_df.to_csv(CSV_PATH, mode='a')
    

        #row_df.to_sql(sqlite_table, engine, if_exists="append", index=False)
        #data_to_db(row_df, engine, sqlite_table)
 #DB
        #engine = create_engine('sqlite:///hltvdb.db', echo=False)

        #data_to_db(row_df, engine, 'tb_matches')

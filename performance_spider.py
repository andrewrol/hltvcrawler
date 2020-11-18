import scrapy
import os
import re
import json
import pandas as pd
import re
import numpy as np
import sqlalchemy
import time
from datetime import date
from datetime import datetime


BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, 'url_dir')
DB_PATH = os.path.join(BASE_DIR, 'hltvdb.db')
CSV_PATH = os.path.join(BASE_DIR, 'matches.csv')
PERFORMANCE_PATH = os.path.join(BASE_DIR, 'performance.csv')
DUEL_PATH = os.path.join(BASE_DIR, 'duel_data.csv')
ERROR_PATH = os.path.join(BASE_DIR, 'error.csv')

try:
    
    matches_df = pd.read_csv(CSV_PATH, encoding='utf-8')
    performance_df = pd.read_csv(PERFORMANCE_PATH, encoding='utf-8')

    match_performance_ids = matches_df['77892'].astype(int)
    performance_ids = performance_df['performance_id'].astype(int)

    performance_urls = match_performance_ids[~match_performance_ids.isin(performance_ids)]
    urls = [f'https://www.hltv.org/stats/matches/performance/{url}/match' for url in performance_urls]
    mapstatsid_urls = [f'https://www.hltv.org/stats/matches/performance/mapstatsid/{url}/match' for url in performance_urls]

    error_urls = pd.read_csv(ERROR_PATH, encoding='utf-8')
    error_urls_list = list(error_urls['url'])

    performance_urls = urls + mapstatsid_urls

    performance_urls = [url for url in performance_urls if url not in error_urls_list]


except:
    pass


class PerformanceSpider(scrapy.Spider):
    name = "performance"

    if 'performance_urls' in globals():
        start_urls = performance_urls
    else:
        start_urls = ['https://www.hltv.org/stats/matches/performance/77907/complexity-vs-fnatic']

    def parse(self, response):

        try:

            def get_player_info(resp):
                
                full_data = []

                for player_data in resp:

                    data = player_data.split('=')[3]
                    data = re.findall(r'{(.*?)}', data)
                    data = ["{"+i+"}" for i in data]
                    data = data[1:-1]
                    data = [json.loads(stat) for stat in data]
                    data = [stat['displayValue'] for stat in data]
                    full_data.append(data)
                
                return full_data

            def get_performance_data(response):

                players_name = response.css('.player-nick::text').getall()
                players_name = [player.lower() for player in players_name]
                player_stats = get_player_info(response.css('.col .highlighted-player .facts .graph').getall())
                performance_df = pd.DataFrame()
                performance_df['nickname'] = players_name
                stats_cols = ['KPR', 'DPR', 'KAST', 'Impact', 'ADR', 'Rating']
                stats_df = pd.DataFrame(player_stats ,columns = stats_cols)
                performance_df[stats_cols] = stats_df
                
                return performance_df
            
            def get_performance_id(response):
                
                selector_string = response.xpath("//div[@class='small-padding stats-detailed-stats']")[0].extract()

                #selector_string = response.xpath("//a[@class='col-box a-reset']")[0].extract()
                performance_id = re.findall(r'"(.*?)"', selector_string)[1].split('/')[3]

                return performance_id
            
            def get_duel_data(response):
                
                t1_data = np.array([int(x) for x in response.css('.team1-player-score::text').getall()[0:25]])
                t2_data = np.array([int(x) for x in response.css('.team2-player-score::text').getall()[0:25]])
                t1_names = response.css('.team1 a::text').getall()[0:5]
                t2_names = response.css('.team2 a::text').getall()[0:5]
                duels = np.subtract(t2_data, t1_data)
                #match_id = get_match_id(response)
                
                x = 0
                duel_data = []

                for player in t2_names:           
                    for i in range(5):
                        duel_data.append([player, t1_names[i], duels[i+x], 'team2'])
                        duel_data.append([t1_names[i], player, -duels[i+x], 'team1'])
                    x += 5
                
                return duel_data
            
            def get_team_data(response):
                
                #teams = response.css('.players-team-header span::text').getall()
                team1 = response.xpath("//th[contains(@class, 'team1-column')]//img[contains(@class, 'team-logo')]/@title").extract()[-1]
                team2 = response.xpath("//th[contains(@class, 'team2-column')]//img[contains(@class, 'team-logo')]/@title").extract()[-1]

                return [team1, team2]

            def match_result(response):
                
                match_result = response.css('.stats-match-map-result-score::text').get()
                match_type = response.css('.stats-match-map-result-mapname::text').get()
                team_names = response.css('.players-team-header span::text').getall()

                return match_result

            if response.status == 429:
                time.sleep(10)
            
            if response.status != 200:
                
                error_list = [response.url, response.status, datetime.now().strftime("%d/%m/%Y %H:%M:%S")]

                error_data = pd.DataFrame(error_list).T
                error_data.columns = ['url', 'status', 'datetime']

                if 'error.csv' in os.listdir(BASE_DIR):

                    error_data.to_csv(ERROR_PATH, mode='a', header=False, index=False)

                else:

                    error_data.to_csv(ERROR_PATH, mode='a', header=True, index=False)       
        
            if 'performance' in response.url:
                
                if 'mapstatsid' in response.url:
                    mapstats = True
                else:
                    mapstats = False

                performance_id = response.url.split('/')[-2]

                performance_data = get_performance_data(response)
                performance_data['performance_id'] = performance_id

                teams = get_team_data(response)
                duel_data = pd.DataFrame(get_duel_data(response))
                duel_cols = ['player1', 'player2', 'duel_result', 'team']
                duel_data['performance_id'] = performance_id
                duel_cols = ['player1', 'player2', 'duel_result', 'team', 'performance_id']
                duel_data.columns = duel_cols

                duel_data['team_name'] = np.where(duel_data['team'] == 'team1', teams[0], teams[1])
                duel_data['mapstatsid'] = mapstats

                if 'duel_data.csv' in os.listdir(BASE_DIR):

                    duel_data.to_csv(DUEL_PATH, mode='a', header=False, index=False)

                else:

                    duel_data.to_csv(DUEL_PATH, mode='a', header=True, index=False)  

                if not performance_data.empty:

                    performance_data['mapstatsid'] = mapstats
                    if 'performance.csv' in os.listdir(BASE_DIR):
                    
                        performance_data.to_csv(PERFORMANCE_PATH, mode='a', header=False, index=False)

                    else:

                        performance_data.to_csv(PERFORMANCE_PATH, mode='a', header=True, index=False)

                else:
                    print('empty df')
            
            elif 'stats' and 'matches' not in response.url:
                
                original_url = response.url.replace(response.url.split('/')[-1], '')
                next_url = response.css('.result-con .a-reset::attr(href)').get()
                stats_url = response.urljoin(next_url)
                
                yield scrapy.Request(stats_url, callback=self.parse)

            elif 'matches' in response.url:

                last_url = response.css('.small-padding a::attr(href)').get()
                perf_id = last_url.split('/')[-2]
                new_url = f'https://www.hltv.org/stats/matches/performance/{perf_id}/match'

                yield scrapy.Request(new_url, callback=self.parse)          
        
        except:
            pass
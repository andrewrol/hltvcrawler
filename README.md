# hltvcrawler
Crawler for hltv rankings, matches and match performances

!DO NOT ABUSE HLTV SERVERS!

These are just crawlers/spiders, so you need to setup a project in scrapy and change its settings
You will also need to scrape 'CS:GO Results' to get the url.txt

This code can be vastly improved on and I still plan to make some changes. The main problem with it is that it saves .csv and .txt files, but you can easily save it to a database.

So here is what the spiders do:

hltv_spider.py ("hltv" in scrapy) gets the absolute rankings of hltv through time
region_spider.py ("region") gets the regional rankings of hltv through time
performance_spider.py ("performance") gets performance stats of players in matches (ADR, DPR, KAST, Rating, etc) and also gets informations from duels (e.g. coldzera is +2 k/d against shox in a certain match, and shox has -2 k/d against coldzera in the same match)
match_spider.py ("matches") gets the match results, date (unix with 3 more zeroes for some reason), and some identification for you to join it later

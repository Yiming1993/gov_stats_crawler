import config
from spider import Spider
import urllib.request
from bs4 import BeautifulSoup
import re

class Pollution_spider(Spider):
    def __init__(self):
        super(Pollution_spider, self).__init__()
        self.main_link = 'http://pm25.in'
        self.db = config.db_path('pollution')

    def get_city_list(self):
        def process_city_list(html):
            soup = BeautifulSoup(html, 'lxml')
            cities = soup.select('ul[class="unstyled"] div li a')

            for i in cities:
                name = re.sub(r'<[^>]+>','',str(i))
                link = re.findall(r'href="[^>]+"',str(i))[0]
                link = re.sub(r'href=','',str(link))
                link = re.sub(r'"','',str(link))
                link = 'http://pm25.in' + link
                self.save_city_list('city_catalog', name, link)

        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=self.main_link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        process_city_list(html)


    def save_city_list(self, collection_name, city_name, city_link):
        _db = config.db_path('pollution')
        exists = _db[collection_name].find({"city":city_name}).count()
        if exists == 0:
            _db[collection_name].insert({"city":city_name,
                                         "link":city_link})
            print("link of city {} is saved".format(city_name))
        else:
            print("link of city {} exists".format(city_name))

    def get_catalog(self, collection_name):
        _db = config.db_path('pollution')
        cities = _db[collection_name].find({})
        links = []
        for doc in cities:
            city_link = doc["link"]
            links.append(city_link)

        return links

    def clean_tabs(self, string):
        string = re.sub(r'<[^>]+>','',str(string))
        return re.sub(r'\s','',str(string))

    def key_clean(self, string):
        return re.sub(r'\.','',str(string))

    def get_air_pollution_data(self, city_link):
        def process_city_pollution(html):
            soup = BeautifulSoup(html,'lxml')
            table_keys = soup.select('th')
            table_values = soup.select('td')
            keys = list(map(self.clean_tabs, table_keys))
            keys = list(map(self.key_clean, keys))
            values = list(map(self.clean_tabs, table_values))
            single_data = {}
            data = []
            values = [values[i:i+len(keys)] for i in range(0, len(values), len(keys))]
            for i in values:
                for j in range(len(i)):
                    single_data[keys[j]] = i[j]
                data.append(single_data)
                single_data = {}
            average_values = soup.select('div[class="span12 data"] div[class="span1"] div[class="value"]')
            average_keys = soup.select('div[class="span12 data"] div[class="span1"] div[class="caption"]')
            average_keys = list(map(self.clean_tabs, average_keys))
            average_keys = list(map(self.key_clean, average_keys))
            average_values = list(map(self.clean_tabs, average_values))
            average_ = list(zip(average_keys, average_values))
            average_data = {i[0]:i[1] for i in average_}

            level = soup.select('div[class="span11"] div[class="level"] h4')
            level = list(map(self.clean_tabs, level))[0]

            update_time = soup.select('div[class="span11"] div[class="live_data_time"] p')
            update_time = list(map(self.clean_tabs, update_time))[0]
            update_value = update_time.split("：")[1]
            update_date = update_value[:10]
            update_time = update_value[10:]

            primary_pollutant = soup.select('div[class="primary_pollutant"] p')
            primary_pollutant = list(map(self.clean_tabs, primary_pollutant))[0].split("：")[1]

            health_affect = soup.select('div[class="affect"] p')
            health_affect = list(map(self.clean_tabs, health_affect))[0].split("：")[1]

            advice = soup.select('div[class="action"] p')
            advice = list(map(self.clean_tabs, advice))[0].split("：")[1]

            city = soup.select('div[class="city_name"] h2')
            city = list(map(self.clean_tabs, city))[0]

            self.save_data('air_pollution', city, average_data, data, level, update_date, update_time, primary_pollutant, health_affect, advice)


        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=city_link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        process_city_pollution(html)

    def save_data(self, collection_name, city, average_data, data, level, update_date, update_time, primary_pollutant, health_affect, advice):
        exist = self.db[collection_name].find({"$and":[
            {"city":city},{"update_date":update_date},{"update_time":update_time}
        ]}).count()
        if exist == 0:
            self.db[collection_name].insert(
                {"city":city,
                 "average_data":average_data,
                 "data":data,
                 "level":level,
                 "update_date":update_date,
                 "update_time":update_time,
                 "primary_pollutant":primary_pollutant,
                 "health_affect":health_affect,
                 "advice":advice
            }
            )
            print('city {} updated in {} at {} is saved'.format(city, update_date, update_time))

        else:
            print('city {} updated in {} at {} exists'.format(city, update_date, update_time))



if __name__ == '__main__':
    P = Pollution_spider()
    links = P.get_catalog('city_catalog')
    for link in links:
        P.get_air_pollution_data(link)
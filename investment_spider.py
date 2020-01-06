from spider import Spider
import config
import urllib.request
import re
from bs4 import BeautifulSoup

class investment_spider(Spider):
    def __init__(self, collection_name, op_type, style_type):
        super(investment_spider, self).__init__()
        self.db = config.db_path('investment')
        self.collection_name = collection_name
        self.first_link = 'http://project.mofcom.gov.cn/sinfo/s_1_0.html?{}&r=&t=ichk=0&starget=1&style={}'
        self.other_link = 'http://project.mofcom.gov.cn/bbsinfo/s_1_0_{}.html?style={}&{}&r=&t=ichk=0&starget=1'

        self.type_dict = {
            'investment_op':'q=field39^%D2%FD%D7%CA',
            'other_investment_op':'q=field39^%D2%FD%D7%CA;field13=%D6%D8%B5%E3',
            'domestic':'1800000091-1-10000101',
            'oversea_op':'q=field39^%B6%D4%CD%E2%CD%B6%D7%CA',
            'other_oversea_op':'q=field39^%B6%D4%CD%E2%CD%B6%D7%CA;field13=%D6%D8%B5%E3',
            'oversea':'1800000091-1-10000106',
            'intention_op':'q=',
            'other_intention_op':'q=field28^%D6%D8%B5%E',
            'intention':'1800000091-5-10000103'
        }
        self.op_type = op_type
        self.style_type = style_type


    def make_link(self, range_num, op_type, style_type):
        if style_type == 'intention':
            self.tag = 'table[class="idModule18"] tbody tr'
        if style_type == 'oversea':
            self.tag = 'table[class="idModule21"] tbody tr'
        if style_type == 'investment':
            self.tag = 'table[class="idModule16"] tbody tr'

        if style_type == 'intention':
            if range_num == 0:
                link = 'http://project.mofcom.gov.cn/bbsinfo/s_5_0.html?style={}&{}&r=&t=ichk=0&starget=1'.format(self.type_dict[style_type], self.type_dict[op_type])
            else:
                link = 'http://project.mofcom.gov.cn/bbsinfo/s_5_0_{}.html?style={}&{}&r=&t=ichk=0&starget=1'.format(range_num+1, self.type_dict[style_type], self.type_dict[op_type])
            return link

        else:
            if range_num == 0:
                link = self.first_link.format(self.type_dict[op_type], self.type_dict[style_type])
                return link
            else:
                link = self.other_link.format(range_num+1, self.type_dict[style_type], self.type_dict[op_type])
                return link



    def investment(self, limit):
        def process_investment(html):
            soup = BeautifulSoup(html,'html5lib')
            data = soup.select('table[class="idModule18"] tbody tr')
            for i in data:

                    data_dict = {}
                    title = i.select('a')[0]
                    link = re.findall(r'href="[^>]+"', str(title))[0].split(' ')[0]
                    link = re.sub(r'href=', '', str(link))
                    link = re.sub(r'"', '', str(link))
                    link = 'http://project.mofcom.gov.cn' + link
                    industry_region = i.select('td[class="zsyzlb-fmk2"]')
                    industry = industry_region[0]
                    region = self.format_control(industry_region[1])
                    if ',' in region:
                        province = region.split(',')[0]
                        city = region.split(',')[1]
                        data_dict['city'] = city
                        data_dict['province'] = province
                    else:
                        data_dict['city'] = '不限'
                        data_dict['province'] = '不限'
                    data_dict["title"] = self.format_control(title)
                    data_dict["link"] = link
                    data_dict["industry"] = self.format_control(industry)

                    data = self.investment_content(link)
                    data["city"] = data_dict["city"]
                    data["province"] = data_dict["province"]
                    data["link"] = data_dict["link"]
                    data["coll_date"] = self.today
                    self.save_investment(data)

                    break

        for i in range(0, limit):
            link = self.make_link(i, self.op_type, self.style_type)
            print(link)
            self.get_random_ip()
            self.header["User-Agent"] = self.random_select_header(self.usrAgent)
            proxy_support = urllib.request.ProxyHandler(self.proxy)
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
            request = urllib.request.Request(url=link, headers=self.header)
            response = urllib.request.urlopen(request, timeout=60)
            html = str(response.read(), 'gbk')
            process_investment(html)
            break

    def investment_content(self, link):
        def process_content(html):
            soup = BeautifulSoup(html,'html5lib')
            data_value = soup.select('table[id="TABLE3"] tbody tr td[class="xmnrmk2"]')[1:]
            data_list = list(map(self.format_control,data_value))
            print(data_list)
            date = re.findall(r"'[^>]+'",str(data_list[1]))[0]
            date = re.sub(r"'",'',date)
            date = date[:-8]
            date = self.date_process(date)

            data_dict = {
                "title":data_list[0],
                "date":date,
                "type":data_list[2],
                "investment_type":data_list[3],
                "industry":data_list[4],
                "location":data_list[5],
                "validation_duration":data_list[6],
                "total_value":data_list[7],
                "investment_value":data_list[8],
                "mark":data_list[9],
                "description":data_list[10],
                "project_type":data_list[11],
                "advantage":data_list[12],
                "est_sales":data_list[13],
                "est_duration":data_list[14],
                "est_employment":data_list[15],
                "environment_description":data_list[16],
                "investor_condition":data_list[17]
            }
            return data_dict


        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'gbk')
        data = process_content(html)
        return data

    def date_process(self,date):
        date = date.split('/')
        month = date[1]
        day = date[2]
        year = date[0]
        if len(month) == 1:
            month = '0' + month
        if len(day) == 1:
            day = '0' + day
        return year + '-' + month + '-' + day

    def save_investment(self, data_dict):
        exist = self.db[self.collection_name].find({'link':data_dict['link']}).count()
        if exist == 0:
            self.db[self.collection_name].insert(data_dict)
            print('the investment %s is saved' %str(data_dict['title']))
        else:
            print('the investment %s exists' %str(data_dict['title']))

if __name__ == '__main__':
    I = investment_spider()
    I.investment(30)


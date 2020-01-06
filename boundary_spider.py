import config
from spider import Spider
import json
import xlwt
from urllib.parse import quote
import urllib.request
import re

class Boundary_spider(Spider):
    def __init__(self, city_name, district_range, db_name):
        super(Boundary_spider, self).__init__()
        # TODO 替换为上面申请的密钥
        self.amap_web_key = ''
        self.poi_search_url = ""
        self.db_name = db_name

        self.city_name = city_name
        self.district_range = district_range
        self.db_name = db_name

    def getpoi_page(self):
        req_url = self.poi_search_url.format(self.amap_web_key, quote(self.city_name), self.district_range)
        with urllib.request.urlopen(req_url) as f:
            data = f.read()
            data = data.decode('utf-8')
        return data

    def data_transfer(self, data):
        poly_line = data.get("polyline")
        poly = []
        poly_line = re.split(r'[|]', poly_line)
        for i in poly_line:
            single_poly_line = i.split(";")
            complete_single_poly_line = [j.split(',') for j in single_poly_line]
            poly.append(complete_single_poly_line)
        data["polyline"] = poly
        return data

    def get_pois(self):
        result = self.getpoi_page()
        result = json.loads(result)  # 将字符串转换为json
        if result['count'] == '0':
            return None
        else:
            for j in result["districts"]:
                data = self.data_transfer(j)
                self.save_poi(self.db_name, 'districts', data)

    def save_poi(self, db_name, collection_name, data):
        _db = config.db_path(db_name)
        exist = _db[collection_name].find({"adcode":data["adcode"]}).count()
        if exist == 0:
            _db[collection_name].insert(data)
            print('poi: {} is saved'.format(data["name"]))
        else:
            print('poi: {} exists'.format(data["name"]))

if __name__ == '__main__':
    district_list = []
    for i in district_list:
        B = Boundary_spider(i, '', '')
        B.get_pois()
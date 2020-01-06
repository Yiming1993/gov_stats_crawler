import config
from spider import Spider
import json
import xlwt
from urllib.parse import quote
import urllib.request

class POI_spider_gaode(Spider):
    def __init__(self, city_name, area_list, amenities_list, db_name):
        super(POI_spider_gaode, self).__init__()
        # TODO 替换为上面申请的密钥
        self.amap_web_key = ''
        self.poi_search_url = "http://restapi.amap.com/v3/place/text"
        self.poi_boundary_url = "https://ditu.amap.com/detail/get/detail"
        self.db_name = db_name

        # TODO cityname为需要爬取的POI所属的城市名，areas为城市下面的所有区，classes为多个分类名集合. (中文名或者代码都可以，代码详见高德地图的POI分类编码表)
        self.cityname = city_name
        self.areas = area_list
        self.classes = amenities_list

    def data_transfer(self, data):
        types = data.get("type").split(';')
        type_1 = types[0]
        type_2 = types[1]
        type_3 = types[2]
        data["type_1"] = type_1
        data["type_2"] = type_2
        data["type_3"] = type_3
        del data["type"]
        try:
            locations = data.get("location").split(",")
            data["location"] = locations
        except:
            pass
        try:
            entr_locations = data.get("entr_location").split(",")
            data["entr_location"] = entr_locations
        except:
            pass
        try:
            exist_locations = data.get("exit_location").split(",")
            data["exit_location"] = exist_locations
        except:
            pass
        return data


    # 根据城市名称和分类关键字获取poi数据
    def get_pois(self, cityname, keywords):
        i = 1
        poilist = []

        while True:  # 使用while循环不断分页获取数据
            result = self.getpoi_page(cityname, keywords, i)
            result = json.loads(result)  # 将字符串转换为json
            if result['count'] == '0':
                break
            for j in result["pois"]:
                data = self.data_transfer(j)
                self.save_poi(self.db_name, 'poi', data)
            self.hand(poilist, result)
            i = i + 1
        return poilist


    # 数据写入excel
    def write_to_excel(self, poilist, cityname, classfield):
        # 一个Workbook对象，这就相当于创建了一个Excel文件
        book = xlwt.Workbook(encoding='utf-8', style_compression=0)
        sheet = book.add_sheet(classfield, cell_overwrite_ok=True)

        # 第一行(列标题)
        sheet.write(0, 0, 'lon_gaode')
        sheet.write(0, 1, 'lat_gaode')
        sheet.write(0, 2, 'count')
        sheet.write(0, 3, 'name')


        for i in range(len(poilist)):
            location = poilist[i]['location']
            name = poilist[i]['name']
            lng = str(location).split(",")[0]
            lat = str(location).split(",")[1]
            sheet.write(i + 1, 0, lng)
            sheet.write(i + 1, 1, lat)
            sheet.write(i + 1, 2, 1)
            sheet.write(i + 1, 3, name)

        book.save(r'' + cityname + "_" + classfield + '.xls')


    # 将返回的poi数据装入集合返回
    def hand(self, poilist, result):
        # result = json.loads(result)  # 将字符串转换为json
        pois = result['pois']
        for i in range(len(pois)):
            poilist.append(pois[i])


    # 单页获取pois
    def getpoi_page(self, cityname, keywords, page):
        req_url = self.poi_search_url + "?key=" + self.amap_web_key + '&extensions=all&keywords=' + quote(
            keywords) + '&city=' + quote(cityname) + '&citylimit=true' + '&offset=25' + '&page=' + str(
            page) + '&output=json'
        data = ''
        with urllib.request.urlopen(req_url) as f:
            data = f.read()
            data = data.decode('utf-8')
        return data

    def save_poi(self, db_name, collection_name, data):
        _db = config.db_path(db_name)
        exist = _db[collection_name].find({"id":data["id"]}).count()
        if exist == 0:
            _db[collection_name].insert(data)
            print('poi: {} is saved'.format(data["name"]))
        else:
            print('poi: {} exists'.format(data["name"]))

    def work_flow(self):
        for clas in self.classes:
            classes_all_pois = []
            for area in self.areas:
                pois_area = self.get_pois(area, clas)
                print('当前城区：' + str(area) + ', 分类：' + str(clas) + ", 总的有" + str(len(pois_area)) + "条数据")
                classes_all_pois.extend(pois_area)
            print("所有城区的数据汇总，总数为：" + str(len(classes_all_pois)))

if __name__ == '__main__':
    P = POI_spider_gaode()
    P.work_flow()

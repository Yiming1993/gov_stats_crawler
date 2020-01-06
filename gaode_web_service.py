import config
import urllib.request
import json
import numpy as np

class Gaode_service(object):
    def __init__(self):
        self.amap_web_key_1 = ''
        self.amap_web_key_2 = ''
        self.amap_web_key_3 = ''
        self.amap_web_key_4 = ''
        self.poi2address = ''
        self.poi2poi = ''



    def poi_address(self, location, radius=100):
        req_url = self.poi2address.format(location, self.amap_web_key_3, radius)
        with urllib.request.urlopen(req_url) as f:
            data = f.read()
            data = data.decode('utf-8')
        data = json.loads(data, encoding='utf-8')
        if data["status"] == "1":
            try:
                address = data["regeocode"]["addressComponent"]
                nearest_location = self._get_nearest(data["regeocode"]["pois"])
                type_ = nearest_location["type"].split(';')
                data_dict = {"id" : nearest_location['id'],
                            "address" : address["streetNumber"]["street"] + address["streetNumber"]["number"],
                            "pname" : address["province"],
                            "citycode" : address["citycode"],
                            "cityname" : address["city"],
                            "adcode" : address["adcode"],
                            "adname" : address["district"],
                            "type_1" : type_[0],
                            "type_2" : type_[1],
                            "type_3" : type_[2],
                             "tel":nearest_location["tel"],
                             "location":location
                        }
                return data_dict
            except:
                print('no nearest poi')
                return {}
        else:
            return {}

    def poi_poi(self, location, cor_type='gps'):
        req_url = self.poi2poi.format(location, self.amap_web_key_4, cor_type)
        with urllib.request.urlopen(req_url) as f:
            data = f.read()
            data = data.decode('utf-8')
        data = json.loads(data, encoding='utf-8')
        if data["status"] == "1":
            locations = data["locations"]
            return locations
        else:
            return location

    def _get_nearest(self, poi_dict):
        distance_list = [float(i["distance"]) for i in poi_dict]
        min_loc = np.argmin(np.array(distance_list))
        return poi_dict[min_loc]

if __name__ == '__main__':
    G = Gaode_service()
    data = G.poi_address('')
    print(data)
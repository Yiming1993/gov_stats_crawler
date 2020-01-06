import config
import fiona
import re
import pyproj
import os
from urllib.parse import unquote
import math


class Geo_data(object):
    def __init__(self, db_name, collection_name):
        self.db_name = db_name
        self.collection_name = collection_name

    def tag_transfer(self, tag_cn):
        if '名称' in tag_cn or '单位名' in tag_cn:
            return 'name'
        elif '类别' in tag_cn:
            return 'type'
        else:
            return tag_cn

    def clean_format(self,string):
        string = str(string)
        string = re.sub(r'\s','',string)
        string = re.sub(r'\xa0','',string)
        string = re.sub(r'\u3000','',string)
        return string

    def read_arcgis(self, file_path, mode = 'read'):
        data = fiona.open(file_path, encoding='gbk')

        for f in data.items():
            properties = f[1]['properties']
            property_dict = {i[0]:i[1] for i in properties.items()}
            poi = f[1]['geometry']
            coordinates = poi["coordinates"]
            if len(coordinates) > 1:
                coordinates = [coordinates]
            property_dict["coordinate_type"] = poi["type"]
            property_dict["coordinates"] = coordinates
            property_dict = {self.tag_transfer(i[0]):self.clean_format(i[1]) for i in property_dict.items()}
            if 'ID' in property_dict.keys():
                del property_dict['ID']
            if '序号' in property_dict.keys():
                del property_dict["序号"]
            property_dict["crs"] = data.crs
            property_dict["crs_wkt"] = data.crs_wkt
            if mode == 'read':
                print(data.crs_wkt)
                self.transfer_coordiantes()
            else:
                self.save_arcgis(self.db_name, self.collection_name, property_dict["name"], property_dict, 'name')

    def transfer_coordiantes(self):
        p1 = pyproj.Proj(init = 'epsg:4326', preserve_units=False)

        p2 = pyproj.Proj(init = 'epsg:3857', preserve_units=False)

        x1, y1 = p1(12962417.045353912,4856663.089266056)
        lon, lat = pyproj.transform(p1,p2,x1,y1)
        print(lon, lat)

    def save_arcgis(self, db_name, collection_name, name, data, find_key):
        _db = config.db_path(db_name)
        _db[collection_name].insert(data)
        print('data with {} {} is saved'.format(find_key, name))

if __name__ == '__main__':
    G = Geo_data('')
    file_list = os.listdir('')
    for i in file_list:
        G.read_arcgis('' + str(i))


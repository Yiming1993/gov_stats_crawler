# coding = 'utf-8'
import config
import matplotlib.pyplot as plt
from text_cluster import Text_cluster as tc
import pandas as pd
from bson import ObjectId
import xlrd
# import shapefile
import numpy as np

class Tools(object):
    def __init__(self):
        self.origin_path, self.db = config.origin_path()


    def data_search(self, collection_name, key_name, value):
        collection_dict = {}
        data = self.db[str(collection_name)].find({key_name:{"$regex":value}})
        temp_list = []
        for j in data:
            item = j[str(key_name)]
            temp_list.append(item)
        collection_dict[str(collection_name)] = temp_list
        return collection_dict

    def cal_data_by_key(self, collection_name, key_name):
        collection_dict = {}
        data = self.db[str(collection_name)].find({})
        temp_list = {}
        for j in data:
            item = j[str(key_name)]
            if type(item) == str:
                if item not in temp_list:
                    temp_list[item] = 1
                else:
                    temp_list[item] = temp_list[item] + 1
            if type(item) == list:
                for x in item:
                    if x not in temp_list:
                        temp_list[str(x)] = 1
                    else:
                        temp_list[str(x)] = temp_list[str(x)] + 1

        collection_dict[str(collection_name)] = sorted(temp_list.items(), key = lambda x:x[1], reverse=True)
        return collection_dict

    def build_pie_graph(self, data_frame, save_image= False, image_title = '默认图像.png'):
        from pylab import mpl
        mpl.rcParams['font.sans-serif'] = ['simhei']
        mpl.rcParams['axes.unicode_minus'] = False
        X = [i[0] for i in data_frame]
        Y = [i[1] for i in data_frame]
        plt.pie(Y, labels=X)
        if save_image == True:
            plt.savefig(image_title)
        plt.axis('equal')
        plt.show()

    def cluster_cata_by_key(self, collection_name, key_name, value, topics_num = 5):
        data = self.db[str(collection_name)].find({key_name:value})
        data_list = []
        for j in data:
            title = j["title"]
            data_list.append(title)
        corpus = tc().gen_corpus(data_list)
        tc().LDA(data_list, corpus, n_topics=topics_num)

    def find_common_dataset(self, coll_1, coll_2):
        set_1 = []
        set_2 = []
        data_1 = self.db[str(coll_1)].find({})
        for i in data_1:
            title = i["title"]
            set_1.append(title)
        data_2 = self.db[str(coll_2)].find({})
        for i in data_2:
            title = i["title"]
            set_2.append(title)
        intersection = list(set(set_1).intersection(set(set_2)))
        return intersection

    def cal_catagories_by_key(self, collection_name, key_name):
        collection_dict = self.cal_data_by_key(collection_name, key_name)
        data_list = collection_dict[str(collection_name)]
        return [i[0] for i in data_list]

    def search_data_by_key(self, collection_name, key_name, show_by_key):
        data = self.db[str(collection_name)].find({})
        temp_list = {}
        for j in data:
            item = j[str(key_name)]
            item_affliate_data = j[str(show_by_key)]
            if type(item) == str:
                if item not in temp_list.keys():
                    temp_list[item] = [item_affliate_data]
                else:
                    initial_list = temp_list[item]
                    initial_list += [item_affliate_data]
                    temp_list[item] = initial_list
            if type(item) == list:
                for x in item:
                    if x not in temp_list.keys():
                        temp_list[str(x)] = [item_affliate_data]
                    else:
                        initial_list = temp_list[x]
                        initial_list += [item_affliate_data]
                        temp_list[x] = initial_list

        return temp_list

    def read_table(self, file_path):
        df = pd.read_csv(file_path, encoding='gbk', header=0)

        for indexs in df.index:
            yield df.loc[indexs].values

    def save_table(self, db_name, collection_name, name, address, district, street, tel):
        _db = config.db_path(db_name)
        exists = _db[collection_name].find({"name":name}).count()
        if exists == 0:
            _db[collection_name].insert({
                "name":name,
                "address":address,
                "district":district,
                "tel":tel,
                "street":street
            })
            print('park {} is saved'.format(name))
        else:
            print('park {} exists'.format(name))

    def update_info(self, db_name, collection_name, _id, tag_name, data):
        _db = config.db_path(db_name)
        _db[str(collection_name)].update({"_id": ObjectId(_id)}, {"$set": {str(tag_name): data}}, True, True)
        print('Object {} is updated with tag {} and data {}'.format(_id, tag_name, data))

    def merge_name(self, name, key_word, add_key = False):
        if key_word in name:
            if add_key == True:
                return name.split(key_word)[0] + key_word
            else:
                return name.split(key_word)[0]
        else:
            return name

    def json2csv(self, file_name, file_path):
        import pandas as pd
        data = pd.read_json(file_path, encoding='utf-8')
        data.to_csv(file_name + '.csv', index=False, encoding='utf-8')

    def check_point_in(self, lon, lat, polygon_region):
        lon = float(lon)
        lat = float(lat)
        iSum = 0
        iCount = len(polygon_region)
        polygon_region = [(float(i[0]), float(i[1])) for i in polygon_region]

        if (iCount < 3):
            return False

        for i in range(iCount):

            pLon1 = polygon_region[i][0]
            pLat1 = polygon_region[i][1]

            if (i == iCount - 1):

                pLon2 = polygon_region[0][0]
                pLat2 = polygon_region[0][1]
            else:
                pLon2 = polygon_region[i + 1][0]
                pLat2 = polygon_region[i + 1][1]

            if ((lat >= pLat1) and (lat < pLat2)) or ((lat >= pLat2) and (lat < pLat1)):

                if (abs(pLat1 - pLat2) > 0):

                    pLon = pLon1 - ((pLon1 - pLon2) * (pLat1 - lat)) / (pLat1 - pLat2)

                    if (pLon < lon):
                        iSum += 1

        if (iSum % 2 != 0):
            return True
        else:
            return False


    def get_polygon(self, db_name, collection_name, district_name):
        _db = config.db_path(db_name)
        polyline = _db[collection_name].find({"name":district_name})
        for doc in polyline:
            polygon = doc["polyline"]
        return polygon

    def get_pois(self, db_name, collection_name, poi_type, type_tag_name, region_name):
        poi_list = []
        _db = config.db_path(db_name)
        pois = _db[collection_name].find({"$and":[{type_tag_name: poi_type},{"adname":region_name}]})
        for doc in pois:
            polygon = doc["location"]
            polygon = [float(polygon[0]), float(polygon[1])]
            poi_list.append(polygon)
        return poi_list

    def get_max_min_poly(self, polygon_region):
        lon_list = [float(i[0]) for i in polygon_region]
        lat_list = [float(i[1]) for i in polygon_region]
        maxlon = np.max(np.array(lon_list))
        minlon = np.min(np.array(lon_list))
        maxlat = np.max(np.array(lat_list))
        minlat = np.min(np.array(lat_list))
        return maxlon, maxlat, minlon, minlat

if __name__ == '__main__':
    T = Tools()

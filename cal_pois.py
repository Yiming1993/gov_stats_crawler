# coding = 'utf-8'
import config
from toolkits import Tools
import datetime
from hashlib import sha256
import random

class Draw_map(Tools):
    def __init__(self):
        super(Draw_map, self).__init__()
        earth_radius_lon = 63569088.0
        self.earth_circumference_lon = float(earth_radius_lon) * float(2.0) * 3.141592657
        self.earth_radius_lat = float(63778300.0)

    def form_request_num(self, region_name, poi_type, window_size, tag_type_level, list_order):
        full_chain = region_name + poi_type + str(window_size) + tag_type_level + str(
            list_order)
        full_chain = ''.join([str(ord(i)) for i in full_chain])
        chain = ''
        sh256 = sha256()
        sh256.update(full_chain.encode('utf-8'))
        res = sh256.hexdigest()
        for i in range(4):
            full_chain = ''.join(res[:4])
            chain += full_chain.upper()
        return chain

    def form_window(self, lon, lat, maxlon, maxlat, minlon, minlat, window_size):
        NW = [minlon + window_size * (lon), maxlat - window_size * (lat)]
        NE = [minlon + window_size * (lon + 1), maxlat - window_size * (lat)]
        SW = [minlon + window_size * (lon), maxlat - window_size * (lat + 1)]
        SE = [minlon + window_size * (lon + 1), maxlat - window_size * (lat + 1)]
        window_poi = [NW, NE, SE, SW]
        window_center = [NW[0] + window_size / 2, NW[1] - window_size / 2]
        return window_poi, window_center

    def form_working_list(self, lon_range, lat_range):
        return [(i,j) for i in range(lon_range) for j in range(lat_range)]

    def save_process_log(self, db_name, order_num, maxlon, maxlat, minlon, minlat, range_lon, range_lat, window_size, polygon_region,  poi_type, tag_type_level):
        _db = config.db_path(db_name)
        exists = _db["orders"].find({"order_num":order_num}).count()
        if exists == 0:
            _db["orders"].insert({
                "order_num":order_num,
                "maxlon": maxlon,
                "maxlat": maxlat,
                "minlon": minlon,
                "minlat": minlat,
                "range_lon":range_lon,
                "range_lat":range_lat,
                "polygon_region":polygon_region,
                "tag_type_level":tag_type_level,
                "poi_type":poi_type,
                "window_size":window_size})
            print('region {} with window {} in poi_type {} is saved'.format(region_name, window_size, poi_type))
        else:
            print("order {} exists".format(order_num))

    def form_request_order(self, db_name, region_name, poi_type, polygon_region, window_size, tag_type_level):
        iterT = 0
        order_num_list = []
        for single_poly in polygon_region:
            maxlon, maxlat, minlon, minlat = self.get_max_min_poly(single_poly)
            range_lon = int(abs((maxlon - minlon)) / window_size) + 1
            range_lat = int(abs((maxlat - minlat)) / window_size) + 1
            order_num = self.form_request_num(region_name, poi_type, window_size, tag_type_level, iterT)

            self.save_process_log(db_name, order_num, maxlon, maxlat, minlon, minlat, range_lon, range_lat, window_size, single_poly, poi_type, tag_type_level)
            iterT += 1

            order_num_list.append(order_num)
        return order_num_list

    def get_breakpoint_data(self, db_name, order_num):
        _db = config.db_path(db_name)
        breakpoint_data = _db["orders"].find({"$and":[{"order_num":order_num},{"status":{"$exists":False}}]})
        for doc in breakpoint_data:
            data = doc
            return data

    def get_order_num(self, db_name):
        _db = config.db_path(db_name)
        breakpoint_data = _db["orders"].find({"status": {"$exists": False}})
        order_nums = []
        for doc in breakpoint_data:
            order_num = doc["order_num"]
            order_nums.append(order_num)
        return order_nums

    def update_breakpoint_data(self, db_name, order_num, last_position, rest_num):
        _db = config.db_path(db_name)
        _db["orders"].update({"order_num":order_num}, {"$set":{"last_position":last_position}}, True, True)
        print('calculation in point {}'.format(rest_num))

    def terminate_order(self, db_name, order_num):
        _db = config.db_path(db_name)
        _db["orders"].update({"order_num": order_num}, {"$set": {"status": "over"}}, True, True)
        print('{} is {} '.format(order_num,"over"))

    def count_poi_num(self, db_name, collection_name, pois, order_num):
        breakpoint_data = self.get_breakpoint_data(db_name, order_num)
        if breakpoint_data != None:
            window_size = breakpoint_data["window_size"]
            maxlon = breakpoint_data["maxlon"]
            maxlat = breakpoint_data["maxlat"]
            minlon = breakpoint_data["minlon"]
            minlat = breakpoint_data["minlat"]
            range_lon = breakpoint_data["range_lon"]
            range_lat = breakpoint_data["range_lat"]
            polygon_region = breakpoint_data["polygon_region"]
            tag_type_level = breakpoint_data["tag_type_level"]
            poi_type = breakpoint_data["poi_type"]

            working_list = self.form_working_list(range_lon, range_lat)
            rest_num = len(working_list)
            try:
                last_position = breakpoint_data["last_position"]
            except:
                last_position = 0

            working_list_ = working_list[last_position:]

            for step in working_list_:
                lon_i = step[0]
                lat_i = step[1]
                window_poi, window_center = self.form_window(lon_i, lat_i, maxlon, maxlat, minlon, minlat,
                                                             window_size)
                if self.check_point_in(window_center[0], window_center[1], polygon_region):
                    in_list = [m for m in pois if self.check_point_in(m[0], m[1], window_poi) == True]
                    num = len(in_list)
                    data = {
                            "center":window_center,
                            "window_poi":window_poi,
                            "num_poi":num,
                            "region":region_name,
                            "type_level":tag_type_level,
                            "type":poi_type,
                            "window_size":window_size
                            }
                    self.save_hotspots(db_name, collection_name, data)
                rest_ = rest_num - working_list.index(step)
                self.update_breakpoint_data('xiamen', order_num, working_list.index(step), rest_)
            self.terminate_order(db_name, order_num)

    def save_hotspots(self, db_name, collection_name, data):
        _db = config.db_path(db_name)
        exist = _db[collection_name].find({"$and":[{"center":data["center"]},{"type":data["type"]},{"type_level":data["type_level"]},{"region":data["region"]}]}).count()

        if exist != 0:
            print('point: {} exists'.format(data["center"]))
        else:
            _db[collection_name].insert(data)
            print('point: {} is saved'.format(data["center"]))
'''
    def save_breakpoint_data(self, db_name, data):
        _db = config.db_path(db_name)
        _db["process_log_test"].remove()
        _db["process_log_test"].insert(data)
        print("breakpoint data saved")

    def lat2distance(self, lat_1, lat_2):
        _lat = abs(lat_1-lat_2)
        _distance = _lat / 360 * self.earth_circumference_lon
        return _distance

    def lon2distance(self, lon_1, lon_2, cos):
        _lon = abs(lon_1-lon_2)
        earth_circumference_lat = cos * self.earth_radius_lat * 2 * 3.141592657
        _distance = _lon / 360 * earth_circumference_lat
        return _distance

    def Pythagoras_triangle(self, side_1, side_2):
        side_3_sqr = side_1**2 + side_2**2
        side_3 = side_3_sqr**0.5
        return side_3

    def cal_distance_poi(self, poi_1, poi_2):
        lon_1 = poi_1[0]
        lon_2 = poi_2[0]
        lat_1 = poi_1[1]
        lat_2 = poi_2[1]
        lat_distace = self.lon2distance(lon_1, lon_2, lat_1)
        lon_distace = self.lat2distance(lat_1, lat_2)
        distace = self.Pythagoras_triangle(lat_distace, lon_distace)
        return distace

    def count_poi_in_range_num(self, db_name, collection_name, region_name, poi_type, polygon_region, window_size, range, pois, tag_type_level):
        for single_poly in polygon_region:
            maxlon, maxlat, minlon, minlat = self.get_max_min_poly(single_poly)
            range_lon = int(abs((maxlon - minlon)) / window_size) + 1
            range_lat = int(abs((maxlat - minlat)) / window_size) + 1
            breakpoint_data = self.get_breakpoint_data(db_name, region_name, poi_type, window_size, tag_type_level)
            if breakpoint_data != None:
                lon_i = breakpoint_data["lon"]
                lat_i = breakpoint_data["lat"]
                NW = [minlon + window_size * (lon_i), maxlat - window_size * (lat_i)]
                NE = [minlon + window_size * (lon_i + 1), maxlat - window_size * (lat_i)]
                SW = [minlon + window_size * (lon_i), maxlat - window_size * (lat_i + 1)]
                SE = [minlon + window_size * (lon_i + 1), maxlat - window_size * (lat_i + 1)]
                window_poi = [NW, NE, SE, SW]
                window_center = [NW[0] + window_size / 2, NW[1] - window_size / 2]
                lon = lon_i
                lat = lat_i
            else:
                NW = [minlon, maxlat]
                NE = [minlon + window_size, maxlat]
                SW = [minlon, maxlat - window_size]
                SE = [minlon + window_size, maxlat - window_size]

                window_poi = [NW, NE, SE, SW]
                window_center = [NW[0] + window_size / 2, NW[1] - window_size / 2]
                lon = 0
                lat = 0

            for lon_i in range(lon, range_lon):
                for lat_i in range(lat, range_lat):
                    if self.check_point_in(window_center[0], window_center[1], polygon_region):
                        in_list = [m for m in pois if self.cal_distance_poi(window_center, m) <= range]
                        num = len(in_list)
                        data = {
                            "center": window_center,
                            "window_poi": window_poi,
                            "num_poi": num,
                            "region": region_name,
                            "type_level": tag_type_level,
                            "type": poi_type,
                            "window_size": window_size
                        }
                        self.save_hotspots(db_name, collection_name, data)
                        # pois = [i for i in pois if i not in remove_list]
                        # if pois == []:
                        #     self.clear_process_log(db_name, region_name, poi_type, window_size, tag_type_level)
                        #     return

                    NW = [minlon + window_size * (lon_i), maxlat - window_size * (lat_i)]
                    NE = [minlon + window_size * (lon_i + 1), maxlat - window_size * (lat_i)]
                    SW = [minlon + window_size * (lon_i), maxlat - window_size * (lat_i + 1)]
                    SE = [minlon + window_size * (lon_i + 1), maxlat - window_size * (lat_i + 1)]
                    window_poi = [NW, NE, SE, SW]
                    window_center = [NW[0] + window_size / 2, NW[1] - window_size / 2]

                    self.break_point_data = {
                        "center": window_center,
                        "window_poi": window_poi,
                        "region": region_name,
                        "type_level": tag_type_level,
                        "type": poi_type,
                        "window_size": window_size,
                        "lon": lon_i,
                        "lat": lat_i
                    }
                    self.save_breakpoint_data('xiamen', self.break_point_data)
            self.clear_process_log(db_name, region_name, poi_type, window_size, tag_type_level)

    def clear_process_log(self, db_name, region_name, poi_type, window_size, tag_type_level):
        _db = config.db_path(db_name)
        _db["process_log_test"].remove({"$and":[
            {"region":region_name},
            {"type":poi_type},
            {"window_size":window_size},
            {"type_level":tag_type_level},
        ]})
        print('breakpoint data {} is removed'.format(str({
            "region":region_name,
            "type":poi_type,
            "window_size":window_size,
            "type_level":tag_type_level})))

    def work_flow_count_poi_num(self):
        try:
            polygon_region = D.get_polygon('','districts','')
            pois = D.get_pois('xiamen', 'poi', '', 'type_1')
            D.count_poi_num('xiamen','hotspot', '', '', polygon_region, 0.001, pois, 'type_1')
        except Exception as e:
            print(e)
            D.save_breakpoint_data('xiamen', D.break_point_data)
            print(str(D.break_point_data))
'''

if __name__ == '__main__':
    D = Draw_map()
    region_name = ''
    poi_type = ''
    type_ = 'type_1'
    polygon_region = D.get_polygon('xiamen', 'districts', region_name)
    pois = D.get_pois('xiamen', 'poi', poi_type, type_, region_name)
    order_nums = D.form_request_order('xiamen',region_name,poi_type, polygon_region, window_size=0.001, tag_type_level=type_)
    for i in order_nums:
        D.count_poi_num('xiamen', 'hotspot', pois, i)
    # if flag == 1:
    # print('{} with poi_type: {} is completed'.format(region_name, poi_type))

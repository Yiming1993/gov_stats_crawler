import config
from pyecharts import Geo, Style
import numpy as np

class Make_map(object):
    def __init__(self):
        pass

    def get_data(self, db_name, collection_name, search_rule, search_spot_name = '_id', search_spot_value = 1):
        self.db = config.db_path(db_name)
        spots = self.db[collection_name].find(search_rule)
        coords = {str(doc[search_spot_name]):doc["location"] for doc in spots}
        spots = self.db[collection_name].find(search_rule)
        attr = []
        value = []
        for doc in spots:
            attr.append(str(doc[search_spot_name]))
            value.append(search_spot_value)

        return coords, attr, value


    def draw_heat_map(self, name, city_name, coords, attr, value):
        geo = Geo(
            name,
            title_color="#fff",
            title_pos="center",
            width=1200,
            height=600,
        )
        geo.add("", attr, value, visual_range=[np.min(np.array(value)), np.max(np.array(value))], symbol_size=5,
                visual_text_color="#fff", is_piecewise=True,
                is_visualmap=True,
                maptype=city_name,
                visual_split_number=10,
                # type="heatmap",
                geo_cities_coords=coords)
        geo.render(name + '.html')

if __name__ == '__main__':
    M = Make_map()
    coords, attr, value = M.get_data()
    M.draw_heat_map('',coords, attr, value)
import config
import re
import jieba
import Levenshtein as lst
import numpy as np
from scipy.spatial.distance import pdist

class Industry_graph(object):
    def __init__(self, db_name):
        self.db = config.db_path(db_name)
        self.origin_path = config.origin_path()
        self.dict_path = self.origin_path + '/References/dict.txt'
        self.stop_path = self.origin_path + '/References/stop.txt'
        jieba.load_userdict(self.dict_path)
        self.stop = [i.strip() for i in open(self.stop_path).readlines()]
        self.keywords = ['制造', '加工', '制作', '生产', '种植', '采运', '采选', '采集', '饲养', '养殖', '捕捞', '开采', '印刷', '冶炼', '修理',
                         '生产', '工程', '活动', '批发', '代理', '零售', '运输', '仓储', '租赁', '出租', '治理', '管理', '维护', '维修', '教育', '出版',
                         '培训', '洗选', '销售', '组装', '组织', '承办']

    def clear_words(self, description):
        cut_list = jieba.lcut(description)
        cut_list_ = [i for i in cut_list if i not in self.keywords]
        cut_list_ = [i for i in cut_list if i not in self.stop]
        return ''.join(cut_list_)

    def get_data_company(self, collection_name, region_name, limit = None):
        data = self.db[collection_name].find({"$and":[{"city":region_name},{"industry":{"$exists":True}}]})
        description_list = []
        iterT = 0
        for doc in data:
            if limit != None and iterT <= limit:
                description = doc["industry_parent"]
                description_list.append(description)
                iterT += 1
            if limit != None and iterT > limit:
                break
            if limit == None:
                description = doc["industry_parent"]
                description_list.append(description)
        return description_list

    def cal_data_poi(self, collection_name, type_name, type_level, data_title):
        data = self.db[collection_name].find({"type_1":str(type_name)})
        cal_dict = {}
        for doc in data:
            type_name_ = doc[str(type_level)]
            if type_name_ not in cal_dict:
                cal_dict[type_name_] = 1
            else:
                cal_dict[type_name_] = cal_dict[type_name_] + 1
        cal_dict["title"] = data_title
        return cal_dict

    def work_flow(self, collection_name, region_name, data_title, limit = None):
        description_list = self.get_data_company(collection_name, region_name, limit = limit)
        cal_list = list(map(self.split_industry, description_list))
        final_dict = {}
        for i in cal_list:
            for j in i.keys():
                if j != '' and j != ' ':
                    if j not in final_dict:
                        final_dict[j] = i[j]
                    else:
                        final_dict[j] = i[j] + final_dict[j]
        final_dict["title"] = data_title
        return final_dict


    def split_industry(self, description):
        description_ = re.sub(r'（[^>]+）', '', str(description))
        description_ = re.sub(r'\([^>]+。', '', description_)
        description_ = re.sub(r'\([^>]+\.\.\.', '', description_)
        description_ = re.split(r'[；、;]', description_)
        description_ = list(map(self.clear_words, description_))

        return {i:description_.count(i) for i in description_}

    def split_industry_list(self, description):
        description_ = re.sub(r'（[^>]+）', '', str(description))
        description_ = re.sub(r'\([^>]+。', '', description_)
        description_ = re.sub(r'\([^>]+\.\.\.', '', description_)
        description_ = re.split(r'[；;]', description_)
        description_ = list(map(self.clear_words, description_))

        return description_

    def similarity(self, str1, str2):
        sim = lst.ratio(str1, str2)
        return sim

    def cos_sim(self, list_1, list_2):
        x = np.array(list_1)
        y = np.array(list_2)
        dist = pdist(np.vstack([x, y]), 'cosine')
        return dist

    def cls_cos_dis(self, list_1, list_2, key_1, key_2, limit = 0.5):
        dist = self.cos_sim(list_1, list_2)
        if dist >= limit:
            return 1, [key_1, key_2]
        else:
            return 0, [key_1, key_2]

    def work_flow_graph(self, collection_name, region_name, limit = None):
        description_list = self.get_data_company(collection_name, region_name, limit = limit)
        cal_list = list(map(self.split_industry, description_list))
        final_list = []
        for i in cal_list:
            for j in i:
                if j not in final_list:
                    final_list.append(j)
        sim_dict = {i:[self.similarity(i, j) for j in final_list] for i in final_list}

    def save_cal_result(self, collection_name, data):
        self.db[collection_name].insert(data)
        print('data {} is saved'.format(data["title"]))


if __name__ == '__main__':
    I = Industry_graph('company')
    final_list = I.work_flow('')
    I.save_cal_result('statistic', final_list)

import config
import xlrd
from gaode_web_service import Gaode_service

class read_table(object):
    def __init__(self, db_name, collection_name, tag_turble_list):
        self.tag_turble_list = tag_turble_list
        self.db = config.db_path(db_name)
        self.collection = collection_name

    def read_xls(self, file_path, sheet_num = 0, default_header_num = 0):
        read_book = xlrd.open_workbook(file_path)
        sheet = read_book.sheets()[sheet_num]
        row_num = sheet.nrows
        col_num = sheet.ncols
        for row in range(row_num):
            key_dict = [sheet.cell_value(default_header_num, col) for col in range(col_num)]
            key_dict = self.tag_transfer(key_dict, self.tag_turble_list)
            data_dict = {key_dict[col]:sheet.cell_value(row, col) for col in range(col_num) if row != default_header_num}
            yield data_dict

    def tag_transfer(self, tag_list, tag_turble_list):
        if tag_turble_list != [] or tag_turble_list != '':
            tag_dict = {i[0]:i[1] for i in tag_turble_list}
            return [tag_dict[i] if i in tag_dict.keys() else i for i in tag_list]
        else:
            return tag_list

    def tag_merge(self, data_dict, new_tag, tag_merge_list, merge_func):
        data_list = [data_dict[str(i)] for i in tag_merge_list]
        data_new = merge_func(data_list)
        for i in tag_merge_list:
            del data_dict[i]
        data_dict[new_tag] = data_new
        return data_dict

    def _combine_merge(self, data_list, merge_symbol=','):
        return merge_symbol.join(data_list)

    def _split_merge(self, data_list, merge_symbol=','):
        return data_list[0].split(merge_symbol)

    def data_screen(self, data_dict, *args):
        pass


    def save_data(self, data, exist_detect_tag = 'name', exist_detect = True):
        if exist_detect == True:
            exist = self.db[self.collection].find({exist_detect_tag:data[exist_detect_tag]}).count()
            if exist == 0:
                self.db[self.collection].insert(data)
                print('data {} is saved'.format(data[exist_detect_tag]))
            else:
                print('data {} exists'.format(data[exist_detect_tag]))
        else:
            self.db[self.collection].insert(data)
            print('data {} is saved'.format(data))

    def date_transfer(self, date, transfer_tag, with_digit_check = True):
        if with_digit_check == True:
            date = date.split(transfer_tag)
            year = date[0]
            if len(date) == 2:
                month = date[1]
                if len(month) < 2:
                    month = '0' + month
                return year + '-' + month

            if len(date) == 3:
                month = date[1]
                day = date[2]
                if len(month) < 2:
                    month = '0' + month
                if len(day) < 2:
                    day = '0' + day
                return year + '-' + month + '-' + day
        else:
            return '-'.join(date.split(transfer_tag))

if __name__ == '__main__':
    from gaode_web_service import Gaode_service
    G = Gaode_service()
    R = read_table()
    origin_path = ''
    for i in R.read_xls():
        if i != {}:
            data_dict = R.tag_merge(i, 'location', ['lon', 'lat'], R._combine_merge)
            data_dict["location"] = G.poi_poi(data_dict["location"])
            data_dict = R.tag_merge(data_dict, 'location', ['location'], R._split_merge)
            # print(data_dict)
            R.save_data(data_dict, exist_detect=False)
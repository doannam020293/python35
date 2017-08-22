import requests
import os
import math
import copy
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import re
import csv
from selenium import webdriver
import pickle
import warnings
from multiprocessing import Process, Value, Array, Pool,freeze_support, Lock, Queue
from functools import partial # ap dungj cho map function trong Pool với 2 tham số truyền vào
from itertools import repeat # ap dungj cho map function trong Pool với 2 tham số truyền vào
from pprint import pprint
import json
import sys
import logging
from collections import defaultdict, OrderedDict
import requests
from nam_basic import nam_to_excel, split_list_to_N_equal_element, create_connect_to_mongo, create_log_file
# import collections


class html_tables(object):
    def __init__(self, source):
        self.url_soup = BeautifulSoup(str(source), "html.parser")
        self.source = source
        # self.url_soup = soup

    def read(self):

        self.tables = []
        self.tables_html = self.url_soup.find_all("table")
        # Parse each table
        try:
            for n in range(0, len(self.tables_html)):
                n_cols = 0
                n_rows = 0

                for row in self.tables_html[n].find_all("tr"):
                    col_tags = row.find_all(["td", "th"])
                    if len(col_tags) > 0:
                        n_rows += 1
                        if len(col_tags) > n_cols:
                            n_cols = len(col_tags)

                # Create dataframe
                df = pd.DataFrame(index=range(0, n_rows), columns=range(0, n_cols))

                # Create list to store rowspan values
                skip_index = [0 for i in range(0, n_cols)]

                # Start by iterating over each row in this table...
                # vị trí index row của giá trị cell khi điền vào dataframe, row_counter sẽ tăng +=1 theo từng còng lặp của từng r
                row_counter = 0
                for row in self.tables_html[n].find_all("tr"):

                    # Skip row if it's blank
                    if len(row.find_all(["td", "th"])) == 0:
                        next

                    else:

                        # Get all cells containing data in this row
                        columns = row.find_all(["td", "th"])
                        col_dim = []
                        row_dim = []
                        col_dim_counter = -1
                        row_dim_counter = -1
                        #  index column trong dataframe
                        col_counter = -1
                        this_skip_index = copy.deepcopy(skip_index)

                        for col in columns:
                            # Determine cell dimensions
                            colspan = col.get("colspan")
                            if colspan is None:
                                col_dim.append(1)
                            else:
                                col_dim.append(int(colspan))
                            col_dim_counter += 1

                            rowspan = col.get("rowspan")
                            if rowspan is None:
                                row_dim.append(1)
                            else:
                                row_dim.append(int(rowspan))
                            row_dim_counter += 1

                            # Adjust column counter
                            if col_counter == -1:
                                col_counter = 0
                            else:
                                col_counter = col_counter + col_dim[col_dim_counter - 1]

                            while skip_index[col_counter] > 0:
                                col_counter += 1

                            # Get cell contents
                            # nếu có thẻ <br>, ta sẽ ngăn cách kết quả bởi dấu ,
                            if col.find('br') is not None:
                                cell_data = list(col.strings)
                                # lọc ra, chỉ lấy những phần tử có chứa kí tự
                                cell_data = [x for x in cell_data if len(x.strip()) > 0]
                                cell_data = ', '.join(cell_data)
                            else:
                                cell_data = col.get_text()

                            # Insert data into cell
                            df.iat[row_counter, col_counter] = cell_data

                            # Record column skipping index
                            if row_dim[row_dim_counter] > 1:
                                this_skip_index[col_counter] = row_dim[row_dim_counter]

                    # Adjust row counter
                    row_counter += 1

                    # Adjust column skipping index
                    skip_index = [i - 1 if i > 0 else i for i in this_skip_index]

                    # Append dataframe to list of tables
                self.tables.append(df)
        except: #  do 1 có trường hợp rowspan nhưng lại k có row nào đầy đủ hết các cột => lỗi, trường hợp này có rowspan hay không cũng  như không, nên ta dùng pd.read_htm thông thường
            df = pd.read_html(str(self.source))[0]
            self.tables.append(df)
        return (self.tables)

    def clean_table(self, case=None, header='first_row', remove_na=False):
        # parameter case: những trường hợp đặt biệt ta cần xử  lý với từng table:  'repeat_rowspan' : ta sẽ tự động fill na bằng ô phía trước, merge_colspan_phat_hanh_the: case đối với bảng phá hành thể, nếu cùng 1 hạn mức, ta sẽ gộp file row với nhau
        # header : ' first_row" : header thông thường, header = 'first_column' thì ta cần phải transpose nó lại

        self.read()
        # ta chỉ làm đối với 1 table
        if len(self.tables)>0:
            table = self.tables[0]
            if case == 'repeat_rowspan':
                # trường hợp rowspan,  sẽ để lặp lại.
                table.fillna(method='ffill', inplace=True)
            elif case == 'merge_colspan_phat_hanh_the':
                # đối với ngoại lệ về hạn mức tín dụng, thì sẽ gặp trường gợp colspan  tại cột thứ 4, hạn mức tín dùng, thì sẽ gộp các dòng về làm 1
                index_col_nan = 3  # tên của column đang cần được xử lý là 3 ( ở đây là column hạn mức tín dụng)
                index_nulls = table[pd.isnull(table[index_col_nan])].index  # có thể là 1 list các index
                for index in list(index_nulls):
                    # index = 4
                    previous_row = table.iloc[index - 1, :].values
                    null_row = table.iloc[index, :].values

                    new_row = list(map(add_string, previous_row, null_row))
                    table.iloc[index - 1, :] = new_row
                    table.dropna(subset=[index_col_nan, ], inplace=True)
            elif case =='lich_su_no_xau_BC_VAY':
                table = table.replace('', np.NaN).ffill()
            elif case=='du_no_hien_tai_BC_VAY':
                index_NH = table[table[0].str.contains('^[0-9]\.', regex=True)].index
                # thời hạn dư nợ
                pattern_thoi_han = re.compile('Dư nợ cho vay trung hạn|Dư nợ cho vay ngắn hạn|Dư nợ cho vay dài hạn|Dư nợ cho vay khác|:')
                index_thoi_han = table[table[0].str.contains(pattern_thoi_han, regex=True)].index
                # loại dư nợ
                #bắt đầu =  space (0 hoặc nhiều space) theo sau là dấu -
                # index = table[table[0].str.contains('^ *-|Dư nợ đủ tiêu chuẩn', regex=True)].index
                index = table[table[0].str.contains('^ *-|Dư nợ đủ tiêu chuẩn|Dư nợ có khả năng mất vốn|Dư nợ cần chú ý', regex=True)].index
                # assert len(index_NH) == len(index_thoi_han), 'Dữ liệu đang đang bị định dạng sai'
                # assert len(index) == len(index_thoi_han), 'Dữ liệu đang đang bị định dạng sai'

                df_output = pd.DataFrame(index=range(len(index)),
                                         columns=['KH', 'ngan_hang', 'thoi_han', 'loai_du_no', 'gia_tri_usd',
                                                  'gia_tri_vnd'])
                for i in range(len(index)):
                    # i =0
                    vnd = table.iloc[index[i], 1]
                    usd = table.iloc[index[i], 2]
                    loai_du_no = table.iloc[index[i], 0]
                    # đối với l loại dư nợ ( vd dư nợ đủ tiêu chuẩn), index của thời hạn sẽ là giá trị gần nhất phía trên indexx của loại dư nợ đó
                    index_NH_current = max(x for x in index_NH if x < index[i])
                    index_thoi_han_current = max(x for x in index_thoi_han if x < index[i])
                    thoi_han = table.iloc[index_thoi_han_current, 0]
                    ngan_hang = table.iloc[index_NH_current, 0]

                    df_output.loc[i, 'KH'] = 'a'  # doan nay chu y
                    df_output.loc[i, 'thoi_han'] = thoi_han
                    df_output.loc[i, 'ngan_hang'] = ngan_hang
                    df_output.loc[i, 'loai_du_no'] = loai_du_no.replace('-','')
                    df_output.loc[i, 'gia_tri_usd'] = usd
                    df_output.loc[i, 'gia_tri_vnd'] = vnd
                table = df_output
            else:
                pass

            if header == 'first_column':
                table = pd.DataFrame(table.values.T)
            # else:  # nếu những trường hợp header = first_row nếu có chứa nan thi cảnh báo
                # if table.isnull().values.any() == True:
                    # warnings.warn("cho chứa NaN value tại file ....")
                    # logger.warning("cho chua NaN value  trong bang tinh trang du no hien tai, tai file ....")
            if case != 'du_no_hien_tai_BC_VAY': # trừ trường hợp case = du_no_hien_tai_BC_VAY, thì còn lại ta sẽ remove header, và lấy dòng đầu tiên làm header
                new_header = table.iloc[0]  # grab the first row for the header
                table = table.iloc[1:,:]  # take the data less the header row
                table.rename(columns=new_header, inplace=True)
            if remove_na == True:
                table.dropna(inplace=True)
        else:
            table = None
        return table
        # trường hợp colspan thì ta sẽ phải transpose để fillna với method = ffill


class Index_doc():
    '''
    class để so sánh số thứ tự của các index, các index sẽ có các medthod như greater than, subtrxtion để kiểm tra 2 index nằm cạnh nhau có liên tiếp hay không
    '''
    def __init__(self, number):
        self.number = number
        self.split_number_self = self.split_by()
        # self.childrent = childrent
        # self.parent = parent
        # để compare các index với nhau, ta dùng chỉ số level, sau đó dùng để chỉ số index . VÍ dụ:  các số la mã level 0, số 1 là level 0, 1.1 là level 2
        # order để so sánh những index cùng level với nhau.
        # self.level,self.order = self.level()
        # __repr__ = __str__
    def __str__(self):
        return self.number
    def __repr__(self):
        return self.number
    def __lt__(self, other):
        '''
        ý utowngr: khi so sánh 2 index với nhau, ta sẽ fill các số 0 để cho độ dài 2 index bằng nhau. sau đó
         ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
        :param other: 
        :return: 
        '''
        split_number_self = self.split_number_self
        split_number_other = other.split_number_self
        # split_number_self = self.split_by
        # split_number_other = other.split_by
        len_self = len(split_number_self)
        len_other = len(split_number_other)
        len_max = max([len_other, len_self])
        split_number_self.extend([0] * (len_max - len(split_number_self)))
        split_number_other.extend([0] * (len_max - len(split_number_other)))
        for i in range(len_max):
            if split_number_self[i] != split_number_other[i]:
                return split_number_self[i] < split_number_other[i]

    def __gt__(self, other):
        '''
        ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
        :param other: 
        :return: 
        '''
        split_number_self = self.split_number_self
        split_number_other = other.split_number_self
        # split_number_self = self.split_by
        # split_number_other = other.split_by
        len_self = len(split_number_self)
        len_other = len(split_number_other)
        len_max = max([len_other, len_self])
        split_number_self.extend([0] * (len_max - len(split_number_self)))
        split_number_other.extend([0] * (len_max - len(split_number_other)))
        for i in range(len_max):
            if split_number_self[i] != split_number_other[i]:
                return split_number_self[i] > split_number_other[i]

    def __sub__(self, other):
        split_number_self = self.split_number_self
        split_number_other = other.split_number_self
        len_self = len(split_number_self)
        len_other = len(split_number_other)
        len_max = max([len_other, len_self])
        split_number_self.extend([0] * (len_max - len(split_number_self)))
        split_number_other.extend([0] * (len_max - len(split_number_other)))
        for i in range(len_max):
            if split_number_self[i] != split_number_other[i]:
                return split_number_self[i] - split_number_other[i]

    def split_by(self):
        number = self.number
        level_regex = re.compile('[1-9]\.')
        if re.search(level_regex, number) is not None:
            split_number = number.split('.')
            split_number = [int(x) for x in split_number if x.isdigit()]
            return split_number



def find_table_between_index(soup, index_next, index_previous=None):
    # tìm thông tin giữa các index chính, nếu có table thì lấy table, nếu k có table thì lấy string,
    next_tag = set(soup.find(text=re.compile(index_next),attrs = {'class':re.compile('.')}).find_all_next('table')) if soup.find(
        text=re.compile(index_next)) is not None else set()

    if index_previous is not None:
        # if len(soup.find_all(text=re.compile(index_previous))) > 1:
        #     logger.warning('co 2 gia tri text thao mãn pattern %s' % (index_previous))
        previous_tag = set(soup.find(text=re.compile(index_previous),attrs = {'class':re.compile('.')}).find_all_previous('table')) if soup.find(
            text=re.compile(index_previous)) is not None else set()
        tag = next_tag.intersection(previous_tag)
        # list(tag)[0]
    else:
        tag = next_tag
    if len(tag) > 1:
        # raise ValueError('tìm được 2 bảng ở đây.')
        print('tìm được 2 bảng ở đây.')
        # logger.warning('tim dc 2 bang  o day. nam giua 2 the %s vaf the %s ' %(index_next, index_previous))
        return list(tag)[-1]
    elif len(tag) == 1:
        return list(tag)[0]
    else:
        print('không  tìm thấy table giữa 2 pattern: %s và pattern %s' %(index_next, index_previous))
        return None


def import_The_Tin_dung_to_mongodb(list_file,lock):
    #connect mongo
    db_cic = create_connect_to_mongo(locahost=True,database='cic')
    coll_the = db_cic['the_tin_dung']
    # find structure. TÌm nhuengx header lơn của file
    for full_filename in list_file:
        try:
            input_db = dict()
            # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
            with open(full_filename, encoding='utf-8') as file:
                soup = BeautifulSoup(file, "html.parser")

            #thông tin k nằm trong table nào
            no_number = soup.find(text=re.compile('Số:'))
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
                1].get_text()
            time_query = time_query.strip()


             # lấy ra những class là bold
            style_scc =soup.style
            list_css = (style_scc.get_text()).split('.')
            list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
            # tạo regex những class là bold
            regex_bold_class = re.compile('|'.join(list_bold_tag))
            # tìm structure của file
            # regex kiểu 1.1, 1.2, II, I, V,...
            index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
            index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
            # index_tags_text = [a.get_text().strip() for a in index_tags]

            regex_thong_tin_nhan_dang = re.compile("thông tin nhận dạng|1\.1")
            regex_table12_card = re.compile('1\.2')
            for i in range(len(index_tags)):
                # i = 0
                record = dict()
                first_tag = index_tags[i]
                next_tag = set(first_tag.find_all_next('table'))
                if i < len(index_tags)-1:
                    second_tag = index_tags[i+1]
                    previous_tag = set(second_tag.find_all_previous('table'))
                    tag = next_tag.intersection(previous_tag)
                else:
                    tag = next_tag
                key = first_tag.get_text()
                # kiểm tra xem có bao nhiêu table giữa các index
                if len(tag) ==0:
                    # nếu k có table thì ta thử tìm text giữ 2
                    next_tag = set(first_tag.find_all_next())
                    previous_tag = set(second_tag.find_all_previous())
                    tag = next_tag.intersection(previous_tag)
                    text = [x.get_text() for x in tag]
                    record = ' '.join(text).strip()
                elif len(tag) ==1:
                    df_table11 = html_tables(list(tag)[0])
                    # từng loại table sẽ có cách lấy table khác nhau
                    if re.search(regex_thong_tin_nhan_dang,key) is not None:
                        # day là table 1.1
                        df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
                        #table chỉ có 1 dòng, nên ta sẽ lấy giá trị đầu tiên của nó
                        record = df_table11.to_dict('records')[0]
                        record['no_number'] = no_number
                        record['header'] =header
                        record['time_query'] = time_query
                        record['full_filename'] = full_filename
                    elif re.search(regex_table12_card,key) is not None:
                        # với table 12 này ta sẽ remove NAN sau khi đã transpose lại  table
                        df_table11 = df_table11.clean_table( remove_na=True)
                        record = df_table11.to_dict('records')
                    else:
                        df_table11 = df_table11.clean_table()
                        record = df_table11.to_dict('records')
                    # record[key] = record
                else:
                    print('nhieu table tai day')
                # nếu dữ liệu thu được giữa cac index khác none thì ghi vào db
                if record != '':
                    key = key.replace('.', '\uff0E')
                    input_db[key] = record
                    input_db['_id'] = full_filename
            with lock:
                # print(full_filename)
                # coll_the = db_cic['the_tin_dung']
                coll_the.insert_one(input_db)
        except Exception as error:
            print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(full_filename,error))

def get_index_tag(soup):
    """
    idea : lấy ra những index của 1 file bằng các tìm những class là bolder trong khai bao internal CSS.  Sau đó kiểm tra việc lấy những index này có thiếu index nào không. 
    :param soup: 
    :return: 
    """
    # tao logger
    logger = logging.getLogger(__name__)
    logger = create_log_file(logger,logfile=r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_output\credit\log file\check_index',)

    # lay index
    style_scc =soup.style
    list_css = (style_scc.get_text()).split('.')
    list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
    # tạo regex những class là bold
    regex_bold_class = re.compile('|'.join(list_bold_tag))
    # tìm structure của file
    # regex kiểu 1.1, 1.2, II, I, V,...
    index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
    level_regex = re.compile('[1-9]\.[1-9]*')
    index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})

    #check xem co thieu index nao khong
    index_tags_text = [a.get_text().strip() for a in index_tags]
    index_list_raw = [re.search(level_regex, a).group() for a in index_tags_text]
    index_list = [Index_doc(a) for a in index_list_raw]
    index_list.sort()
    a = 'check index cho list sau: {}'.format(index_list)
    logger.debug(a)
    index_list.sort()
    for i in range(len(index_list)):
        if i >1:
            check = (index_list[i] -index_list[i-1])
            if check !=1:
                a  = 'thieu index giua 2 index sau: {} va {} cua file name: {}'.format(index_list[i],index_list[i-1],full_filename)
                logger.debug(a)
                print(a)
            else:
                logger.debug('chuan')
    return index_tags

def import_html_to_mongodb_without_lock(list_file,lock):
    db_cic = create_connect_to_mongo(locahost=True,database='cic')
    coll_the = db_cic['vay']
    # for full_filename in list_file:
    #     try:
    input_db = dict()
    full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\6\36.html'
    with open(full_filename, encoding='utf-8') as file:
        soup = BeautifulSoup(file, "html.parser")

    #thông tin k nằm trong table nào
    no_number = soup.find(text=re.compile('Số:'))
    header = soup.find('span', attrs={'class': 'headerfont'}).text
    time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
        1].get_text()
    time_query = time_query.strip()
    #find structure. TÌm nhuengx header lơn của file
    index_tags = get_index_tag(soup)
    index_tags_text = [a.get_text().strip() for a in index_tags]

    regex_thong_tin_nhan_dang = re.compile("thông tin nhận dạng|1\.1")
    regex_table12_card = re.compile('1\.2')
    # lặp giữa các index để lấy ra thông tin giữa 2 index nằm cạnh nhau
    for i in range(len(index_tags)):
        # i = 0
        record = dict()
        first_tag = index_tags[i]
        next_tag = set(first_tag.find_all_next('table'))
        if i < len(index_tags)-1:
            second_tag = index_tags[i+1]
            previous_tag = set(second_tag.find_all_previous('table'))
            tag = next_tag.intersection(previous_tag)
        else:
            tag = next_tag
        key = first_tag.get_text()
        # kiểm tra xem có bao nhiêu table giữa các index
        if len(tag) ==0:
            # nếu k có table thì ta thử tìm text giữ 2
            next_tag = set(first_tag.find_all_next())
            previous_tag = set(second_tag.find_all_previous())
            tag = next_tag.intersection(previous_tag)
            text = [x.get_text() for x in tag]
            record = ' '.join(text).strip()
        elif len(tag) ==1:
            df_table11 = html_tables(list(tag)[0])
            # từng loại table sẽ có cách lấy table khác nhau
            if re.search(regex_thong_tin_nhan_dang,key) is not None:
                # day là table 1.1
                df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
                #table chỉ có 1 dòng, nên ta sẽ lấy giá trị đầu tiên của nó
                record = df_table11.to_dict('records')[0]
                record['no_number'] = no_number
                record['header'] =header
                record['time_query'] = time_query
                record['full_filename'] = full_filename
            elif re.search(regex_table12_card,key) is not None:
                # với table 12 này ta sẽ remove NAN sau khi đã transpose lại  table
                df_table11 = df_table11.clean_table( remove_na=True)
                record = df_table11.to_dict('records')
            else:
                df_table11 = df_table11.clean_table()
                record = df_table11.to_dict('records')
            # record[key] = record
        else:
            print('nhieu table tai day')
        # nếu dữ liệu thu được giữa cac index khác none thì ghi vào db
        if record != '':
            key = key.replace('.', '\uff0E')
            input_db[key] = record
            input_db['_id'] = full_filename
    # with lock:
        # print(full_filename)
        # coll_the = db_cic['the_tin_dung']
    coll_the.insert_one(input_db)
        # except Exception as error:
        #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(full_filename,error))

def import_Vay_to_mongodb(list_file,lock):
    #connect mongo
    db_cic = create_connect_to_mongo(locahost=True,database='cic')
    coll_the = db_cic['vay']
    # find structure. TÌm nhuengx header lơn của file
    for full_filename in list_file:
        try:
            input_db = dict()
            # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
            with open(full_filename, encoding='utf-8') as file:
                soup = BeautifulSoup(file, "html.parser")

            #thông tin k nằm trong table nào
            no_number = soup.find(text=re.compile('Số:'))
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
                1].get_text()
            time_query = time_query.strip()


             # lấy ra những class là bold
            style_scc =soup.style
            list_css = (style_scc.get_text()).split('.')
            list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
            # tạo regex những class là bold
            regex_bold_class = re.compile('|'.join(list_bold_tag))
            # tìm structure của file
            # regex kiểu 1.1, 1.2, II, I, V,...
            index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
            index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
            # index_tags_text = [a.get_text().strip() for a in index_tags]

            regex_thong_tin_nhan_dang = re.compile("thông tin nhận dạng|1\.1")
            regex_table12_card = re.compile('1\.2')
            for i in range(len(index_tags)):
                # i = 0
                record = dict()
                first_tag = index_tags[i]
                next_tag = set(first_tag.find_all_next('table'))
                if i < len(index_tags)-1:
                    second_tag = index_tags[i+1]
                    previous_tag = set(second_tag.find_all_previous('table'))
                    tag = next_tag.intersection(previous_tag)
                else:
                    tag = next_tag
                key = first_tag.get_text()
                # kiểm tra xem có bao nhiêu table giữa các index
                if len(tag) ==0:
                    # nếu k có table thì ta thử tìm text giữ 2
                    next_tag = set(first_tag.find_all_next())
                    previous_tag = set(second_tag.find_all_previous())
                    tag = next_tag.intersection(previous_tag)
                    text = [x.get_text() for x in tag]
                    record = ' '.join(text).strip()
                elif len(tag) ==1:
                    df_table11 = html_tables(list(tag)[0])
                    # từng loại table sẽ có cách lấy table khác nhau
                    if re.search(regex_thong_tin_nhan_dang,key) is not None:
                        # day là table 1.1
                        df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
                        #table chỉ có 1 dòng, nên ta sẽ lấy giá trị đầu tiên của nó
                        record = df_table11.to_dict('records')[0]
                        record['no_number'] = no_number
                        record['header'] =header
                        record['time_query'] = time_query
                        record['full_filename'] = full_filename
                    elif re.search(regex_table12_card,key) is not None:
                        # với table 12 này ta sẽ remove NAN sau khi đã transpose lại  table
                        df_table11 = df_table11.clean_table( remove_na=True)
                        record = df_table11.to_dict('records')
                    else:
                        df_table11 = df_table11.clean_table()
                        record = df_table11.to_dict('records')
                    # record[key] = record
                else:
                    print('nhieu table tai day')
                # nếu dữ liệu thu được giữa cac index khác none thì ghi vào db
                if record != '':
                    key = key.replace('.', '\uff0E')
                    input_db[key] = record
                    input_db['_id'] = full_filename
            with lock:
                # print(full_filename)
                # coll_the = db_cic['the_tin_dung']
                coll_the.insert_one(input_db)
        except Exception as error:
            print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(full_filename,error))

def run_TTD2108():

    folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
    folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
    folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
    list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
    list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
    list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
    list_vay = list_vay1 +list_vay2+list_vay3
    # num = Value('i', 0)
    lock = Lock()
    #chia list_vay thành 6 phần. bằng function được định nghĩa trong nam_basic
    processes = [Process(target=import_The_Tin_dung_to_mongodb, args=(list_file,lock)) for list_file in split_list_to_N_equal_element(list_vay,6)]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()



if __name__ == '__main__':
    run_TTD2108()
    # import_html_to_mongodb_without_lock(1,1)
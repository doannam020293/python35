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
from nam_basic import nam_to_excel, split_list_to_N_equal_element, create_connect_to_mongo
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


def import_html_to_mongodb():
    # find structure. TÌm nhuengx header lơn của file
    full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
    with open(full_filename, encoding='utf-8') as file:
        soup = BeautifulSoup(file, "html.parser")

     # lấy ra những class là bold
    style_scc =soup.style
    list_css = (style_scc.get_text()).split('.')
    list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
    # tạo regex những class là bold
    regex_bold_class = re.compile('|'.join(list_bold_tag))
    # soup.find_all(attrs = {'class':'headerfontConCha'})
    # tìm structure của file
    # regex kiểu 1.1, 1.2, II, I, V,...
    index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
    index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
    index_tags_text = [a.get_text().strip() for a in index_tags]
    # indexs_raw = [re.search(index_regex,a).group() for a in index_tags_text]
    # indexs = [Index_doc(a) for a in indexs_raw]
    # indexs.sort()
    # dict_index_tag = dict(zip(indexs,index_tags))
    # od = OrderedDict(sorted(dict_index_tag.items()))




    for i in range(len(index_tags)):
        # i = 1
        first_tag = index_tags[i]
        second_tag = index_tags[i+1]
        next_tag = set(first_tag.find_all_next('table'))
        previous_tag = set(second_tag.find_all_previous('table'))
        tag = next_tag.intersection(previous_tag)
        if len(tag) ==0:
            # nếu k có table thì ta thử tìm text giữ 2
            next_tag = set(first_tag.find_all_next())
            previous_tag = set(second_tag.find_all_previous())
            tag = next_tag.intersection(previous_tag)
            text = [x.get_text() for x in tag]
            text = ' '.join(text).strip()
        elif len(tag) ==1:
            key = first_tag.get_text()
            dict_table  = dict({key:list(tag)[0]})
            df_table11 = html_tables(list(tag)[0])
            # với table này ta sẽ remove NAN sau khi đã transpose lại  table
            df_table11 = df_table11.clean_table(header='first_column', remove_na=True)

            records = json.loads(df_table11.T.to_json()).values()

    db_cic = create_connect_to_mongo(locahost=True,database='cic')





if __name__ == '__main__':
    # logger = logging.getLogger(__name__)
    # create_log_file('log_credit')
    # run_TTD_add_more()
    # logger.info('Hello baby')
    # import_THE_TIN_DUNG_1808(1,1)
    # run_TTD1808()
    # import_THE_TIN_DUNG_1808_single(1,1)

    x = Index_doc('1.')
    # y = index('2', 2)
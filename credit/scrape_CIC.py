# commit 1
import logging
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
import os
import sys
from collections import defaultdict
import requests
from nam_basic import nam_to_excel
from nam_basic import split_list_to_N_equal_element




def add_string(a, b):
    a = '' if pd.isnull(a) else str(a)
    b = '' if pd.isnull(b) else str(b)
    return a + ', ' + b


def find_table_containt_string(soup, string):
    # string = 'Số'
    next_tag = soup.find(text=re.compile(string)).find_parent('table')
    return next_tag


def find_table_between_index(soup, index_next, index_previous=None):
    # tim the 1.2, sau do tim table tag nam sau the nay.
    # ý tưởng, tìm những thẻ table đằng sau nằm giữa 2 thẻ chứa 1 chỉ mục nào đó, ví dụ nằm giữa 1.2 và 1.3
    # thực ra có thể tự tìm các pattern 1.2, 1.3 ,... sau đó chạy vòng lặp theo thứ tự

    # neu không tìm ra table thì sẽ tag sẽ là thẻ rỗng

    # index_next = '^3\.'
    # index_previous = '^4\.'
    #kiểm tra nếu có nhiều mẫu thỏa mãn, thì sẽ log lại
    # if len(soup.find_all(text=re.compile(index_next),attrs = {'class':re.compile('.')})) >1:
    #     logger.warning('co 2 gia tri text thao mãn pattern %s' %(index_next))
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



def concatenate_tables(table1, df2):
    # nối 2 dataframe có cùng header với nhau
    df_concate = pd.concat([df1, df2])
    return df_concate

def import_THE_TIN_DUNG_add_more(lock, list_vay):

    file_name3A = r'C:\nam\work\learn_tensorflow\credit\output\27_07_the_td\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG A.csv'
    # driver = webdriver.Firefox()
    directory0 = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'

    for filename in list_vay:
        # try:
            # filename = '213835.html'
        full_filename = os.path.join(directory0,filename)
        with open(full_filename, encoding='utf-8') as file:
            print(filename)
            soup = BeautifulSoup(file, "html.parser")
        table1 = find_table_between_index(soup, '^1\.1', '^1\.2')
        time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
            1].get_text()
        time_query = time_query.strip()

        df_table1 = html_tables(table1)
        # với table này ta sẽ remove NAN sau khi đã transpose lại  table
        df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
        # cic_id = df_table1['Mã CIC:'].values[0]
        cic_id = df_table1.filter(regex=(".*CIC.*")).values[0]

        infor3 = soup.find(text=re.compile('^3\.'),attrs = {'class':re.compile('.')}).get_text()
        df_table3 = pd.DataFrame([infor3],columns=['giatri'])

        with lock:
            add_infor_to_df_and_write_cvs(df_table3, file_name3A, cic_id=cic_id, time_query=time_query, filename=filename)
        # except Exception as error:
        #     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
        #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))


def import_THE_TIN_DUNG_1808(lock, list_vay):
    file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
    file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
    file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
    file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
    file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
    file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
    file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
    file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
    file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'

    error_list = []

    for filename in list_vay:
        try:
            # filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
            with open(filename, encoding='utf-8') as file:
                print(filename)
                soup = BeautifulSoup(file, "html.parser")

            table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
            table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
            table13 = find_table_between_index(soup, '^1\.3', '^2\.')
            table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
            table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
            table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')

            table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
            df_table11 = html_tables(table11)
            # với table này ta sẽ remove NAN sau khi đã transpose lại  table
            df_table11 = df_table11.clean_table(header='first_column', remove_na=True)

            no_number = soup.find(text=re.compile('Số:'))
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
                1].get_text()
            time_query = time_query.strip()



            # cic_id = df_table1['Mã số CIC:'].values[0]
            cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]

            df_table12 = html_tables(table12)
            # với table này ta sẽ remove NAN sau khi đã transpose lại  table
            df_table12 = df_table12.clean_table( remove_na=True)

            df_table21 = html_tables(table21)
            df_table21 = df_table21.clean_table()


            df_table21 = html_tables(table21)
            df_table21 = df_table21.clean_table()

            df_table22a = html_tables(table22a)
            df_table22a = df_table22a.clean_table()
            try: # thi thoảng ta k có table 22b
                df_table22b = html_tables(table22b)
                df_table22b = df_table22a.clean_table()
            except:
                df_table22b = None
            infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
            df_table3 = pd.DataFrame([infor3], columns=['giatri'])
            #
            # add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
            #                               time_query=time_query, filename=filename)
            # add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
            # add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
            # add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )


            with lock:
                add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
                                              time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
        except Exception as error:
            # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
            print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))


def import_THE_TIN_DUNG_1808_single(lock, list_vay):
    '''
    function cho chay từng file riêng lẻ
    :param lock: 
    :param list_vay: 
    :return: 
    '''
    file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
    file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
    file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
    file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
    file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
    file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
    file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
    file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
    file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'

    error_list = []

    # for filename in list_vay:
    #     try:
    filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\100162.html'
    with open(filename, encoding='utf-8') as file:
        print(filename)
        soup = BeautifulSoup(file, "html.parser")

    table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
    table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
    table13 = find_table_between_index(soup, '^1\.3', '^2\.')
    table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
    table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
    table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')

    table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
    df_table11 = html_tables(table11)
    # với table này ta sẽ remove NAN sau khi đã transpose lại  table
    df_table11 = df_table11.clean_table(header='first_column', remove_na=True)

    no_number = soup.find(text=re.compile('Số:'))
    header = soup.find('span', attrs={'class': 'headerfont'}).text
    time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
        1].get_text()
    time_query = time_query.strip()



    # cic_id = df_table1['Mã số CIC:'].values[0]
    cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]

    df_table12 = html_tables(table12)
    # với table này ta sẽ remove NAN sau khi đã transpose lại  table
    df_table12 = df_table12.clean_table( remove_na=True)

    df_table21 = html_tables(table21)
    df_table21 = df_table21.clean_table()


    df_table21 = html_tables(table21)
    df_table21 = df_table21.clean_table()

    df_table22a = html_tables(table22a)
    df_table22a = df_table22a.clean_table()
    try: # thi thoảng ta k có table 22b
        df_table22b = html_tables(table22b)
        df_table22b = df_table22a.clean_table()
    except:
        df_table22b = None
    infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
    df_table3 = pd.DataFrame([infor3], columns=['giatri'])

    add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
                                  time_query=time_query, filename=filename)
    add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
    add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
    add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
    add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
    add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )

    #
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
    #                                   time_query=time_query, filename=filename)
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
    # with lock:
    #     add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# except Exception as error:
#     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))

def create_log_file(logfile='logfile'):
    global logger
    # logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('%s.log' %(logfile))
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARNING)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

def import_BAO_CAO_VAY(lock,list_vay):
    # global logger
    file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
    file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
    file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
    file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
    file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
    file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\2_5.csv'
    file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
    file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'

    error_list = []
    driver = webdriver.Firefox()
    # print(filename_input)
    # list_vay = [1,]
    directory0 = r'C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/'

    for filename in list_vay:
        try:
        # filename = '141284.html'
        #     logger.info('bat dau ghi file %s' % (filename))
            file_dir_for_browser = 'file:///' + directory0 + filename
            driver.get(file_dir_for_browser)
            source = driver.page_source
            # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
            # # driver.get(file_dir)
            # source = driver.page_source
            # voi 1 vong lap, thi k can phai quit driver.
            soup = BeautifulSoup(source, "html.parser")

            # table4 = find_table_between_index(soup, '^3\.', '^4\.')
            table3 = find_table_between_index(soup, '^3\.', '^4\.')
            if soup.find(text=re.compile('2\.5')) is not None:
                table25 = find_table_between_index(soup, '2\.5', '^3\.')
                table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
            else:
                table25 = None
                table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
            table23 = find_table_between_index(soup, '2\.3', '2\.4')
            table22 = find_table_between_index(soup, '2\.2', '2\.3')
            table21 = find_table_between_index(soup, '2\.1', '2\.2')
            table1 = find_table_between_index(soup, '^1\.', '^2\.')
            table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')

            no_number = soup.find(text=re.compile('Số:'))
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
                1].get_text()
            time_query = time_query.strip()

            df_table1 = html_tables(table1)
            # với table này ta sẽ remove NAN sau khi đã transpose lại  table
            df_table1 = df_table1.clean_table(header='first_column', remove_na=True)

            cic_id = df_table1['Mã số CIC:'].values[0]

            df_table21 = html_tables(table21)
            df_table21 = df_table21.clean_table()

            df_table22 = html_tables(table22)
            df_table22 = df_table22.clean_table()

            df_table23 = html_tables(table23)
            df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')

            df_table24 = html_tables(table24)
            df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')

            # if table25 is not None:
            df_table25 = html_tables(table25)
            df_table25 = df_table25.clean_table()

            df_table3 = html_tables(table3)
            df_table3 = df_table3.clean_table()

            with lock:
                add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
                                              time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)

            # add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
            #                                   time_query=time_query, filename=filename)
            # add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
            # add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query,filename=filename )
            # add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query,filename=filename )
            # logger.info('da ghi file thanh cong %s' % (filename))
        except Exception as error:
            # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
            print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))



def import_BAO_CAO_VAY_1808(lock,list_vay):
    '''
    function chay cho file vay đã được correct, nên chỉ cần mở file bình  thường lên và dùng beautifulsoup
    :param lock: 
    :param list_vay: 
    :return: 
    '''
    # global logger
    file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
    file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
    file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
    file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
    file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
    file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\2_5.csv'
    file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
    file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'

    error_list = []

    for filename in list_vay:
        try:
        # filename = '141284.html'
        #     logger.info('bat dau ghi file %s' % (filename))
        #     file_dir_for_browser = 'file:///' + directory0 + filename
        #     driver.get(file_dir_for_browser)
        #     source = driver.page_source
            # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
            # # driver.get(file_dir)
            # source = driver.page_source
            # voi 1 vong lap, thi k can phai quit driver.
            with open(filename, encoding='utf-8') as file:
                print(filename)
                soup = BeautifulSoup(file, "html.parser")

            # table4 = find_table_between_index(soup, '^3\.', '^4\.')
            table3 = find_table_between_index(soup, '^3\.', '^4\.')
            if soup.find(text=re.compile('2\.5')) is not None:
                table25 = find_table_between_index(soup, '2\.5', '^3\.')
                table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
            else:
                table25 = None
                table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
            table23 = find_table_between_index(soup, '2\.3', '2\.4')
            table22 = find_table_between_index(soup, '2\.2', '2\.3')
            table21 = find_table_between_index(soup, '2\.1', '2\.2')
            table1 = find_table_between_index(soup, '^1\.', '^2\.')
            table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')

            no_number = soup.find(text=re.compile('Số:'))
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
                1].get_text()
            time_query = time_query.strip()

            df_table1 = html_tables(table1)
            # với table này ta sẽ remove NAN sau khi đã transpose lại  table
            df_table1 = df_table1.clean_table(header='first_column', remove_na=True)

            cic_id = df_table1['Mã số CIC:'].values[0]

            df_table21 = html_tables(table21)
            df_table21 = df_table21.clean_table()

            df_table22 = html_tables(table22)
            df_table22 = df_table22.clean_table()

            df_table23 = html_tables(table23)
            df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')

            df_table24 = html_tables(table24)
            df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')

            # if table25 is not None:
            df_table25 = html_tables(table25)
            df_table25 = df_table25.clean_table()

            df_table3 = html_tables(table3)
            df_table3 = df_table3.clean_table()

            with lock:
                add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
                                              time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
            with lock:
                add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
            with lock:
                add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)
        except Exception as error:
            # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
            print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))





def run_TTD1808():
    # global logger
    # N = len(list_vay)
    folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
    folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
    folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
    list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
    list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
    list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
    list_vay = list_vay1 +list_vay2+list_vay3
    lock = Lock()
    processes = [Process(target=import_THE_TIN_DUNG_1808, args=(lock,files)) for files in split_list_to_N_equal_element(list_vay)]
    # proc = processes[0]
    # proc.start()
    # import_THE_TIN_DUNG(lock,list_vay)
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()
def run_TTD():
    # global logger
    # N = len(list_vay)
    folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
    list_vay = [file for file in os.listdir(folder_input)]
    part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
    lock = Lock()
    processes = [Process(target=import_THE_TIN_DUNG, args=(lock,file)) for file in part_list]
    # proc = processes[0]
    # proc.start()
    # import_THE_TIN_DUNG(lock,list_vay)
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()

def run_TTD_add_more():
    # global logger
    # N = len(list_vay)
    folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
    list_vay = [file for file in os.listdir(folder_input)]
    part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
    lock = Lock()
    processes = [Process(target=import_THE_TIN_DUNG_add_more, args=(lock,file)) for file in part_list]
    # proc = processes[0]
    # proc.start()
    # import_THE_TIN_DUNG_add_more(lock,list_vay)
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()

def run_vay():
    # global logger
    # N = len(list_vay)
    folder_input = r'C:\nam\work\learn_tensorflow\credit\input\Vay_the_nhan'
    list_vay = [file for file in os.listdir(folder_input)]
    part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
    lock = Lock()
    processes = [Process(target=import_BAO_CAO_VAY, args=(lock,file)) for file in part_list]
    # proc = processes[0]
    # proc.start()
    # import_BAO_CAO_VAY(lock,list_vay)
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()
    # results_bank = []
def add_infor_to_df_and_write_cvs(df, output_file, **kwargs):
    if df is not None:
        for key, value in kwargs.items():
            df[key] = value
        pd.DataFrame.sort_index(df,axis=1,inplace=True)
        df.to_csv(output_file, encoding='utf-16', sep='\t', mode='a', index=False)
        print('ghi file thanh cong')
        # pd.DataFrame.to_csv()

def clean_phone_number():
    # tim dict map ma cung va tinh

    dict_phone = pickle.load(open(r'C:\nam\work\learn_tensorflow\dict_phone.pickle','rb'))

    # lam sach gia tri
    df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\20.07\thong tin nhan dang.xlsx')
    # df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\clean_sdt20_07.xlsx')
    columns = ['co_quan', 'cic_id', 'ngay_cap', 'quoc_tich', 'cmnd', 'name', 'number', 'address']
    df.columns = columns
    # check lại số diện thoại bắt đầu  = +84 hoặc 84
    df['fixed_phone'] = df['number'].apply(apply_clean_phone_number, args=('fixed phone',))
    df['mobile_phone'] = df['number'].apply(apply_clean_phone_number, args=('mobile phone',))
    df['other'] = df['number'].apply(apply_clean_phone_number, args=('other',))
    df['ma_vung'] = df['fixed_phone'].apply(apply_map_pattern, args = (dict_phone,))

    nam_to_excel(df, 'clean_sdt24_15h')

if __name__ == '__main__':
    # logger = logging.getLogger(__name__)
    # create_log_file('log_credit')
    # run_TTD_add_more()
    # logger.info('Hello baby')
    # import_THE_TIN_DUNG_1808(1,1)
    run_TTD1808()
    # import_THE_TIN_DUNG_1808_single(1,1)

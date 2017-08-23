from bs4 import BeautifulSoup as bs
import os
import pandas as pd
import pickle
import re
import pandas as pd
import numpy as np
import re
from collections import defaultdict
from nam_basic import *
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
import re
import pickle
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import math
from collections import Counter
import datetime
# from nam_basic import nam_to_excel



def tf(word, document):
    words_list = document.strip().split(' ')
    counter = dict(Counter(words_list))
    tf = counter[word] / len(words_list)
    return tf

def idf(word, document, documnet_list):
    number_doc_contain_word = sum([1 for document in documnet_list if word in document])
    idf = math.log(len(documnet_list) /number_doc_contain_word)
    return idf

def if_idf(document, documnet_list):
    result = [tf(word, document)* idf(word, document, documnet_list) for word in document.strip().split(' ')]
    return result

def return_top_important_term(document, documnet_list, n=2):
    # parameter: document, va list document , n: số từ quan trong nhất cần được lấy ra
    # return: n từ quan trọng nhất trong document đó
    list_word = list(zip(if_idf(document, documnet_list), document.split(' ')))
    list_word = sorted(list_word,reverse=True)
    list_word_filter = [word[1] for word in list_word[:n]]
    # important_words = " ".join(list_word)
    # #  sắp xếp lại cho đúng thứ tự xuất hiện trong document. THỰC RA BƯỚC NÀY KHÔNG CẦN, VÌ NẾU 2 TỪ KHÔNG CẠNH NHAU THÌ K CẦN SẮP XẾP
    # document_filter = [word for word in document.split(" ") if word in important_words ]
    return list_word_filter

def find_header():
    ''' tìm header của các file'''
    # header_set = set(())
    header_set = ('BÁO CÁO THÔNG TIN THẺ TÍN DỤNG CÁ NHÂN', 'BÁO CÁO CHI TIẾT VỀ KHÁCH HÀNG VAY THỂ NHÂN')
    the_tin_dung = []
    vay = []
    header_dict = {header_set[0]: the_tin_dung, header_set[1]: vay}

    for filename in os.listdir(directory):
        file_dir = os.path.join(directory, filename)
        with open(file_dir, 'r', encoding="utf8") as file:
            soup = bs(file, 'html.parser')
            header = soup.find('span', attrs={'class': 'headerfont'}).text
            header = header.strip()
            header_dict[header].append(filename)
    return header_dict

def find_collectoral():
    ''' tìm header của các file'''
    directory = r'C:\nam\work\learn_tensorflow\credit\DataFiles'
    header_set_raw = set(())
    header_set = ('BÁO CÁO THÔNG TIN THẺ TÍN DỤNG CÁ NHÂN', 'BÁO CÁO CHI TIẾT VỀ KHÁCH HÀNG VAY THỂ NHÂN')
    pkl_file = open(r'C:\nam\work\learn_tensorflow\credit\header.pickle', 'rb')
    data1 = pickle.load(pkl_file)
    filename_list = data1[header_set[0]]
    list_have_col = []
    for filename in filename_list:
        file_dir = os.path.join(directory, filename)
        with open(file_dir, 'r', encoding="utf8") as file:
            file_name = r'C:/nam/work/learn_tensorflow/credit/DataFiles/data_984.html'
            file = open(file_name, 'r', encoding="utf8")
            soup = bs(file , 'html.parser')
            text_list = soup.find(text=re.compile(r'Thông tin tài sản đảm bảo')).parent
            text = list(text_list.next_siblings)[1]
            text.next_sibling

            if re.search('Chủ thẻ tín dụng có tài sản đảm bảo tại Ngân hàng',text) is not None:
                list_have_col.append(filename) # 984
            # text = text_list.strip()
            header_set_raw.add(text)
            # header_set.add(header)
    return header_dict
def nam():
    file_name = r'C:/nam/work/learn_tensorflow/credit/DataFiles/data_14.html'
    file =  open(file_name, 'r', encoding="utf8")
    soup = bs(file, 'html.parser')
    #thong tin 1
    # a = soup(text=re.compile(r'^[1-4]\.[0-9]'))
    #tìm những đoạn text có chứa đoạn pattern bắt đầu bằng những chữ số từ 1 đến 4 theo sao là dấu . và tiếp theo là chữ số.
    text_list = soup.find_all(text=re.compile(r'^[1-4]\.'))
    text_list = soup.find_all(text=re.compile(r'1.1. Thông tin nhận dạng'))
    for text in text_list:
        text = text_list[1]
        tag = text.parent.next_sibling
    table_data = [[cell.text for cell in row("td")]
                  for row in soup("tr")]
    import json
    print(json.dumps(dict(table_data)))

def check_condition_to_split():
    # nếu len của các phần tử sau khi split có pattern giống như là 1 số điện thoại thì ta sẽ split
    return
def classify_fixedphone_mobiphone(phone):
    # parameter phone: number like a phone, phải bắt đầu bằng chữ số 0 ( vì đoạn sau có check len = 10 hoặc 11
    #return: classification that is fixed phone or mobi phone.
    '''086 088 089 là số vietel'''  #=========================================
    '''086 088 089 là số vietel'''  #=========================================
    '''086 088 089 là số vietel'''  #=========================================
    '''086 088 089 là số vietel'''  #=========================================
    pattern_fix = re.compile('^[2-8]|^[0][2-8]')
    pattern_mobile = re.compile('^[19]|^[0][19]')
    if re.search(pattern_mobile, phone) is not None and len(phone) in [9,10,11]:
        phone_classification = 'mobile phone'
    elif re.search(pattern_fix, phone) is not None and len(phone) in [8,9,10,11]:
        phone_classification = 'fixed phone'
    else:
        phone_classification = 'other'
    return phone_classification

def apply_clean_phone_number(row,name_column):
    # row = '07103/839008 / 0909363692 071 03766669'
    # remove các kí tự không phải là số, ngoại trừ 2 trường hợp dau / va dau space dau -
    # pattern_number = re.compile('[^0-9/ EXT()]', re.IGNORECASE)
    # row = '84 9 04117727'
    pattern_number = re.compile('[^0-9/ -]', re.IGNORECASE) # remove luôn cả chữ ext và dấu đóng mở ngoặc
    row = re.sub(pattern_number,"",str(row))
    #thi thoang co doan can split = space, xem xet lai doan code.
    #split = dau / hoac dau space ( dau cach), dấu -

    list_number  = [numb for numb in re.split('/| |-',row) if len(numb)>0]
    dict_phone = defaultdict(list)
    for i, number in enumerate(list_number):
        # number ='84939800227'
        # lam sach sdt, them so 0 vao dau, hoặc thay thế 84 = số 0
        if re.search(re.compile('^84'), number) is not None:
            number = number.replace('84', '0', 1)
            # nếu có nhiều hơn 1 số 0 ở đầu, thì sẽ replace nó
            number = re.sub(re.compile('^00+'), "0", str(number))
            # number = number.replace('84', '0', 1)
        # add so 0 vào đầu
        if re.search(re.compile('^0'), number) is None:
            number = '0' + number  # removw list cu bangf phan tu nayd
        # do 1 sdt co the vua split bằng / hoặc space, nên ta sẽ chỉ áp dụng split nếu kết quả sau khi split là 1 số cố định hoặc 1 số điện thoại, nếu không ta sẽ gộp 2 kết quả split lại, thử xem nó có là số điện thoại hay không
        if classify_fixedphone_mobiphone(number) != 'other':
            dict_phone[classify_fixedphone_mobiphone(number)].append(number)
        elif i>0 and classify_fixedphone_mobiphone(list_number[i-1] + list_number[i]) != 'other': # check điều kiện i>0 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[classify_fixedphone_mobiphone(list_number[i-1] + list_number[i])].append(list_number[i-1] + list_number[i])
        elif i>1 and classify_fixedphone_mobiphone(list_number[i-2]+list_number[i-1] + list_number[i]) != 'other': # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[classify_fixedphone_mobiphone(list_number[i-2]+list_number[i-1] + list_number[i])].append(list_number[i-2]+list_number[i-1] + list_number[i])
        elif i > 2 and classify_fixedphone_mobiphone(list_number[i - 3]+list_number[i - 2]+list_number[i - 1] + list_number[
            i]) != 'other':  # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[classify_fixedphone_mobiphone(list_number[i - 3]+list_number[i - 2] + list_number[i - 1] + list_number[i])].append(
                list_number[i - 3]+ list_number[i - 2] + list_number[i - 1] + list_number[i])
        else:
            pass
    for key, value in dict_phone.items():
        dict_phone[key] = ','.join(value) if len(value) > 0 else None
    return dict_phone[name_column] if len(dict_phone[name_column]) >0 else None

def apply_add_number_0(number):
    if re.search(re.compile('^0'), str(number)) is None:
        number = '0' + str(number)  # removw list cu bangf phan tu nayd
    return number
def apply_map_pattern(rows, dict_phone):
    # rows la 1 list
    # rows = '0838159438'
    if rows is not None:
        rows = [row for row in rows.split(',')]
        result = [value for row in rows
                  for key, value in dict_phone.items()
                  if re.match(key, row) is not None]
        # tạo list unique, do set() chỉ lấy unique giá trị
        result = list(set(result))
        result = ','.join(result) if len(result) > 0 else None
    else:
        result = None
    return result

def create_dict_phone():
    df_phone = pd.read_html('https://tinhte.vn/threads/danh-sach-ma-vung-so-dien-thoai-cua-59-63-tinh-thanh-tu-1-3-2015.2411166/')[0]
    pd.DataFrame.drop(df_phone,0,inplace=True)
    df1 = df_phone[[3,4,5]]
    df1.columns = ['province','old_number','new_number']
    df2 = df_phone[[0,1,2]]
    df2.columns = ['province','old_number','new_number']
    df_phone = pd.concat([df1,df2])
    df_phone['old_number'] =df_phone['old_number'].apply(apply_add_number_0)
    dict_phone = dict(zip(df_phone.old_number, df_phone.province))
    with open('dict_phone.pickle','wb') as f:
        pickle.dump(dict_phone,f)
    return dict_phone


def clean_phone_number():
    # tim dict map ma cung va tinh

    dict_phone = pickle.load(open(r'C:\nam\work\learn_tensorflow\dict_phone.pickle','rb'))

    # lam sach gia tri
    df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\clean\thong tin nhan dang the.xlsx')
    # df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\clean_sdt20_07.xlsx')
    # columns = ['cic_id', 'quoc_tich', 'cmnd', 'name', 'filename','header', 'no_number', 'time_query', 'number','address']
    columns = ['cic_id', 'cmnd', 'name', 'filename','header', 'no_number', 'time_query', 'number','address']

    df.columns = columns
    # check lại số diện thoại bắt đầu  = +84 hoặc 84
    df['fixed_phone'] = df['number'].apply(apply_clean_phone_number, args=('fixed phone',))
    df['mobile_phone'] = df['number'].apply(apply_clean_phone_number, args=('mobile phone',))
    df['other'] = df['number'].apply(apply_clean_phone_number, args=('other',))
    df['ma_vung'] = df['fixed_phone'].apply(apply_map_pattern, args = (dict_phone,))

    nam_to_excel(df, 'clean_2308_sdt.xlsx')
    nam_to_excel(df, r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\clean\clean_2308_sdt_the.xlsx')

def apply_status_card(row):
    patter_inactive = re.compile('dong the|đóng thẻ|dong thẻ|đong the|the', re.IGNORECASE|re.UNICODE)
    if re.search(patter_inactive,row) is not None :
        result = 'inactive'
    else:
        result = 'active'
    return result

def apply_concate(x):
    a = [str(i) for i in x]
    return ','.join(a)


def check_table_phat_hanh_the():
    # GỘP LẠI NHỮNG TRƯỜNG HỢP MÀ ĐANG BỊ 1 TỔ CHỨC TÍN DỤNG CÓ NHIỀU THẺ
    df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\20.07\thong tin to chuc phat hanh.xlsx')
    columns = ['limit', 'stt', 'card_name', 'bank', 'cic_id', 'filename', 'time_query']
    df.columns = columns
    df['status_card'] = df['card_name'].apply(apply_status_card)

    # df = pd.read_pickle(r'C:\nam\work\learn_tensorflow\df.pcikle')
    f = {'card_name': apply_concate,'bank': apply_concate }
    # df.groupby(['limit','cic_id','filename','status_card','time_query']).agg(f)
    df_tong_hop = df.groupby(['filename','cic_id','time_query','status_card','limit'], as_index=False).agg(f)
    return df_tong_hop

def map_ten_ngan_hang():
    # cac case dac biet: index 317, 321, 430 can split car / va space, sau khi  co so di dong,  dieu kien split, neu len cuar so dang sau 9,10 check len(sdt0 = 10 hoac 11 so hay k

    a  = pd.read_html('https://vi.wikipedia.org/wiki/Danh_s%C3%A1ch_ng%C3%A2n_h%C3%A0ng_t%E1%BA%A1i_Vi%E1%BB%87t_Nam',header=0) # header là dòng đầu tiên
    a_0 = a[2]
    a_1 = a[3]
    a_2 = a[4]
    pd.DataFrame.rename(a_2,columns={'Ngân hàng':'Tên ngân hàng'},inplace=True)
    nhtm_df= pd.concat([a_0,a_1, a_2.head(6)])
    nhtm_df.drop(0,axis = 1 , inplace =True)
    name_nhtm = nhtm_df['Tên ngân hàng'].values
    with open('name_nhtm.pickle','wb') as f:
        pickle.dump(name_nhtm,f)
    name_nhtm = pickle.load(open(r'C:\nam\work\learn_tensorflow\name_nhtm.pickle','rb'))
    part_important_in_nhtm = [return_top_important_term(nhtm, name_nhtm) for nhtm in name_nhtm]

    dict_map_nhtm = [re.compile("%s|%s" %(a,b)) for a,b in part_important_in_nhtm ]
    dict_map_nhtm = [re.compile("%s|%s" %(a,b)) for a,b in part_important_in_nhtm ]
    for a,b in part_important_in_nhtm:
        print(a,b)
        for a, b in item:
            print("%s|%s" %(a,b))
    dict_map_nhtm = [re.compile(a,b) for item in name_nhtm for a,b in item]

    print(part_important_in_nhtm)
    print(name_nhtm)
    x = 'Ngân hàng Citi Bank Việt Nam - Chi nhánh TP.Hồ Chí Minh'

    df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\20.07\thong tin to chuc phat hanh.xlsx')
    columns = ['limit', 'stt', 'card_name', 'bank', 'cic_id', 'filename', 'time_query']
    df.columns = columns
    df['status_card'] = df['card_name'].apply(apply_status_card)
    df.to_pickle('df.pcikle')

def clean_dien_bien_du_no():
    df = pd.read_csv(r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\2_5.csv',encoding='utf-16',sep='\t',header=None,)
    for index in range(len(df)):
        # import ipdb
        # ipdb.set_trace()
        if index%2 ==0:
            print(index)
            data = df.iloc[index +1 , :].values
            columns = df.iloc[index , :].values
            # columns = row.values
            columns1 = ['' if pd.isnull(x) else x.replace("Tháng","").strip() for x in columns ]
            columns2 =  [
                datetime.date(int(a.split('/')[1]),int(a.split('/')[0]), 1)
                if len(a.split('/')) > 1
                else a for a in columns1 ]
            df1 = pd.DataFrame(data=data.reshape(1, 16), columns=columns2)
            try:
                if len(df1.columns):
                    df_new = pd.concat([df_new,df1])
                    # df_new = pd.concat([df_new,df1])
            except:
                df_new = df1
    df_new.to_csv(r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\2_5_new.csv', encoding='utf-16', sep='\t', index=False)


if __name__ == '__main__':
    # clean_date()
    # clean_phone_number()
    # apply_clean_phone_number('0975-503-170','1')
    clean_phone_number()



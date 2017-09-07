from pymongo import MongoClient
from bson.code import Code
from nam_basic import create_connect_to_mongo, nam_to_excel
import pandas as pd
import re
from bson.son import SON
import datetime
from collections import defaultdict


def check_unique_key(coll, name_parent_key=None):
    '''

    :param name_parent_key  ( nếu chỉ định name_parent_key thì sẽ tìm distinct sub key trong parent key đó ( nếu k chỉ định thì ta sẽ tìm distinct key của collection đó 
    :param coll: collection  
    :return: cho 1 parent key, tim all unique nested key trong key do  
    '''

    if name_parent_key is not None:
        # name_parent_key = '2_2_danh_sach_TCTD_da_quan_he'
        # coll = coll_the
        # check value cua parent_key là list(array) hay dict(object), nếu là array thì ta sẽ lấy dict(object) đầu tiên, sau đó sẽ tìm các key của dict(object) này
        first_doc = coll.find_one({name_parent_key: {'$exists': 1}})[name_parent_key]
        if isinstance(first_doc, dict):
            string_code = '''
                    function() {
                              for (var key in this['%s']) { emit(key, null); }
                           }
            ''' % (name_parent_key)
            mapper = Code(string_code)
        elif isinstance(first_doc, list):
            string_code = '''
                    function() {
                            var a = this['%s']
                            for (var key in a) {for (var key1 in a[key]) {emit(key1, null);}}
                           }
            ''' % (name_parent_key)
            mapper = Code(string_code)
            # mapper1 = Code(string_code1)
        else:
            pass
    else:
        mapper = Code("""
            function() {
                          for (var key in this) { emit(key, null); }
                       }
        """)

    reducer = Code("""
        function(key, stuff) { return null; }
    """)

    distinctThingFields = coll.map_reduce(mapper, reducer
                                          , out={'inline': 1}
                                          , full_response=True)
    list_distinct_field = [a['_id'] for a in distinctThingFields['results']]
    return list_distinct_field


def classify_fixedphone_mobiphone(phone):
    # parameter phone: number like a phone, phải bắt đầu bằng chữ số 0 ( vì đoạn sau có check len = 10 hoặc 11
    # return: classification that is fixed phone or mobi phone.
    '''086 088 089 là số vietel'''  # =========================================
    '''086 088 089 là số vietel'''  # =========================================
    '''086 088 089 là số vietel'''  # =========================================
    '''086 088 089 là số vietel'''  # =========================================
    pattern_fix = re.compile('^[2-8]|^[0][2-8]')
    pattern_mobile = re.compile('^[19]|^[0][19]')
    if re.search(pattern_mobile, phone) is not None and len(phone) in [9, 10, 11]:
        phone_classification = 'mobile phone'
    elif re.search(pattern_fix, phone) is not None and len(phone) in [8, 9, 10, 11]:
        phone_classification = 'fixed phone'
    else:
        phone_classification = 'other'
    return phone_classification


def apply_clean_phone_number(row):
    # row = '07103/839008 / 0909363692 071 03766669'
    # remove các kí tự không phải là số, ngoại trừ 2 trường hợp dau / va dau space dau -
    # pattern_number = re.compile('[^0-9/ EXT()]', re.IGNORECASE)
    # row = '84 9 04117727'
    pattern_number = re.compile('[^0-9/ -]', re.IGNORECASE)  # remove luôn cả chữ ext và dấu đóng mở ngoặc
    row = re.sub(pattern_number, "", str(row))
    # thi thoang co doan can split = space, xem xet lai doan code.
    # split = dau / hoac dau space ( dau cach), dấu -

    list_number = [numb for numb in re.split('/| |-', row) if len(numb) > 0]
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
        elif i > 0 and classify_fixedphone_mobiphone(list_number[i - 1] + list_number[
            i]) != 'other':  # check điều kiện i>0 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[classify_fixedphone_mobiphone(list_number[i - 1] + list_number[i])].append(
                list_number[i - 1] + list_number[i])
        elif i > 1 and classify_fixedphone_mobiphone(list_number[i - 2] + list_number[i - 1] + list_number[
            i]) != 'other':  # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[
                classify_fixedphone_mobiphone(list_number[i - 2] + list_number[i - 1] + list_number[i])].append(
                list_number[i - 2] + list_number[i - 1] + list_number[i])
        elif i > 2 and classify_fixedphone_mobiphone(
                                        list_number[i - 3] + list_number[i - 2] + list_number[i - 1] + list_number[
                    i]) != 'other':  # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
            dict_phone[classify_fixedphone_mobiphone(
                list_number[i - 3] + list_number[i - 2] + list_number[i - 1] + list_number[i])].append(
                list_number[i - 3] + list_number[i - 2] + list_number[i - 1] + list_number[i])
        else:
            pass
    # for key, value in dict_phone.items():
    #     dict_phone[key] = ','.join(value) if len(value) > 0 else None
    return dict_phone


def update_key_with_regex(collection, regex_sub_key, new_key, primary_key=None, batch_number=1000):
    '''
    rename key (hoặc subkey trong trong 1 primary key nao do với trường hợp chỉ định primary_key not None) match regex    
    :param collection: collection
    :param primary_key: key chua subkey cần update ( default : None
    :param regex_sub_key:  regex cua sub_key cần được replace bằng new_key. VD: re.compile('nam')
    :param new_key: 
    :param batch_number:  Số lượng câu lệnh cần update theo batch 
    :return: 
    '''
    # update field with regex
    # collection = db['the1']
    # new_key = '1_1_thong_tin_nhan_dang_nam_lan5'
    bulk = collection.initialize_ordered_bulk_op()
    counter = 0
    cusor = collection.find()
    for doc in cusor:
        # doc = list(cusor)[0]
        for k in doc:
            # k = '1_1_thong_tin_nhan_dang_nam_lan2'
            if primary_key is not None:
                if re.search(primary_key, k) is not None:
                    sub_doc = doc[k]
                    for sub_key in sub_doc:
                        if re.search(regex_sub_key, sub_key) is not None:
                            print('match')
                            unset_key = k + '.' + sub_key
                            set_key = k + '.' + new_key
                            bulk.find({"_id": doc['_id']}).update_one(
                                {"$unset": {unset_key: 1}, "$set": {set_key: sub_doc[sub_key]}})
                            counter += 1
                    # sau khi đã tìm được các subkey trong doc[key] thì break cho giảm số vòng lặp
                    break
            else:
                if re.search(regex_sub_key, k) is not None:
                    print('match')
                    bulk.find({"_id": doc['_id']}).update_one({"$unset": {k: 1}, "$set": {new_key: doc[k]}})
                    counter += 1

        if counter % batch_number == 0:  # update sau khi duyet 1000 document
            try:
                bulk.execute()
                print('update thanh cong')
                bulk = collection.initialize_ordered_bulk_op()
            except Exception as error:
                print(error)
    if counter % batch_number != 0:  # chạy batch cho phần còn lại (mà không chia hết cho batch_number)
        try:
            bulk.execute()
            print('update thanh cong')
        except Exception as error:
            print(error)


def correct_key_3_tong_du_no_tin_dung(collection, batch_number=1000):
    # chi ap dung cho rieng thang 3
    regex = re.compile('TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ')
    new_key = '3_tong_du_no_tin_dung'
    bulk = collection.initialize_ordered_bulk_op()
    counter = 0
    cusor = collection.find()
    for doc in cusor:
        # doc = list(cusor)[0]
        for k in doc:
            # k = '1_1_thong_tin_nhan_dang_nam_lan2'
            if re.search(regex, k) is not None:
                value = k.split(':')[-1]
                vnd = value.split(';')[0]
                usd = value.split(';')[-1]
                value_update = dict({'vnd': vnd, 'usd': usd})
                print('match')
                bulk.find({"_id": doc['_id']}).update_one({"$unset": {k: 1}, "$set": {new_key: value_update}})
                counter += 1

        if counter % batch_number == 0:  # update sau khi duyet 1000 document
            try:
                bulk.execute()
                print('update thanh cong')
                bulk = collection.initialize_ordered_bulk_op()
            except Exception as error:
                print(error)
    if counter % batch_number != 0:  # chạy batch cho phần còn lại (mà không chia hết cho batch_number)
        try:
            bulk.execute()
            print('update thanh cong')
        except Exception as error:
            print(error)


def import_coll_to_server():
    # chuyển collection từ local lên server
    a = list(coll_the.find())
    serverdb = create_connect_to_mongo(database='CleanData')
    # query= list(db['the_tin_dung'].find())
    coll_the_server = serverdb['cic_the_tin_dung']
    coll_the_server.insert_many(a)


def clean_vay_part1():
    # clean vay
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_vay = db['vay_clean']

    # check key
    field_all = check_unique_key(coll_vay)
    pd.DataFrame(field_all).to_clipboard()

    # rename key : 1．1． Thông tin nhận dạng => 1_1_thong_tin_nhan_dang
    # list_dict_

    coll_vay.update_many({}, {'$rename': {'1． THÔNG TIN CHUNG VỀ KHÁCH HÀNG   ': '1_thong_tin_nhan_dang'}})

    # get những subkey của primary key : "1_1_thong_tin_nhan_dang"
    distinctThingFields = check_unique_key(coll_vay, '1_thong_tin_nhan_dang')

    # đối những subkey trong primary key 1_1_thong_tin_nhan_dang có tên gần giống nhau thành 1 tên thống nhất
    # rename key : 1_1_thong_tin_nhan_dang.Mã số CIC: => 1_1_thong_tin_nhan_dang.Mã CIC:
    coll_vay.update_many({}, {'$rename': {'1_thong_tin_nhan_dang.Mã số CIC:': '1_thong_tin_nhan_dang.cic_id'}})
    update_key_with_regex(collection=coll_vay, regex_sub_key=re.compile('chứng minh nhân dân'), new_key='phone_number',
                          primary_key='1_thong_tin_nhan_dang')
    update_key_with_regex(collection=coll_vay, regex_sub_key=re.compile('Tên khách hàng'), new_key='name_customer',
                          primary_key='1_thong_tin_nhan_dang')
    update_key_with_regex(coll_vay, re.compile('ịa chỉ'), 'address', '1_thong_tin_nhan_dang')

    coll_vay.update_many({}, {'$rename': {'2．1 Tổng hợp dư nợ hiện tại': '2_1_tong_hop_du_no_hien_tai'}})
    coll_vay.update_many({},
                         {'$rename': {'2．2 Danh sách Tổ chức tín dụng đang quan hệ ': '2_2_danh_sach_TCTD_quan_he'}})
    coll_vay.update_many({},
                         {'$rename': {'2．2 Danh sách Tổ chức tín dụng đã từng quan hệ ': '2_2_danh_sach_TCTD_quan_he'}})
    coll_vay.update_many({}, {
        '$rename': {'2．2．	Chi tiết về nợ vay (không bao gồm nợ thẻ tín dụng)': '2_2_chi_tiet_vay_no'}})
    coll_vay.update_many({}, {'$rename': {'2．3 Tình trạng dư nợ tín dụng hiện tại': '2_2_chi_tiet_vay_no'}})

    coll_vay.update_many({}, {
        '$rename': {'2．3 Thông tin Thẻ tín dụng và dư nợ thẻ tín dụng': '2_3_thong_tin_the_tin_dung'}})
    coll_vay.update_many({}, {'$rename': {' 2．4 Lịch sử nợ xấu 5 năm gần nhất ': '2_4_lich_su_no_nau_5_nam'}})
    coll_vay.update_many({}, {'$rename': {' 2．4 Lịch sử nợ xấu 5 năm gần nhất  ': '2_4_lich_su_no_nau_5_nam'}})
    update_key_with_regex(collection=coll_vay, regex_sub_key=re.compile('VAMC'), new_key='du_no_thuoc_VAMC')

    coll_vay.update_many({}, {
        '$rename': {' 2．5 Nợ cần chú ý trong vòng 12 tháng gần nhất ': '2_5_no_can_chu_y_trong_12_thang'}})
    coll_vay.update_many({}, {
        '$rename': {' 2．5 Nợ cần chú ý trong vòng 12 tháng gần nhất  ': '2_5_no_can_chu_y_trong_12_thang'}})

    coll_vay.update_many({},
                         {'$rename': {' 2．8． Nợ cần chú ý trong vòng 12 tháng gần nhất ': '2_8_no_can_chu_y_12_thang'}})

    coll_vay.update_many({}, {
        '$rename': {'  2．6．	Lịch sử nợ xấu tín dụng trong 05 năm gần nhất ': '2_6_lich_su_no_xau_5_nam'}})
    coll_vay.update_many({}, {
        '$rename': {' 2．6．	Lịch sử nợ xấu tín dụng trong 05 năm gần nhất ': '2_6_lich_su_no_xau_5_nam'}})

    coll_vay.update_many({}, {'$rename': {
        '2．7．	Lịch sử chậm thanh toán thẻ tín dụng trong 03 năm gần nhất': '2_7_lich_su_cham_thanh_toan_3_nam'}})

    coll_vay.update_many({}, {'$rename': {
        '3． DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG (trong 1 năm gần nhất)': '3_danh_sach_tctd_tra_cuu'}})
    coll_vay.update_many({}, {'$rename': {
        '3． DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG (trong 1 năm gần nhất) ': '3_danh_sach_tctd_tra_cuu'}})
    coll_vay.update_many({}, {'$rename': {
        '3．3． DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG (trong 1 năm gần nhất)': '3_danh_sach_tctd_tra_cuu'}})

    coll_vay.update_many({}, {'$rename': {'3．1 Thông tin về tài sản đảm bảo': '3_1_thong_tin_tsdb'}})

    coll_vay.update_many({}, {'$rename': {'3．2 Thông tin về hợp đồng tín dụng': '3_2_thong_tin_hop_dong_tin_dung'}})
    coll_vay.update_many({}, {'$rename': {'3．2 Thông tin về hợp đồng tín dụng ': '3_2_thong_tin_hop_dong_tin_dung'}})

    # Xoa field .đa số các giá trị của 2.2 Lịch sử chậm thanh toán thẻ của chủ thẻ đều có 2 bảng sub ở trong, nên ta sẽ k cần key này nữa
    coll_vay.update_many({'2．1 Diễn biến dư nợ 12 tháng gần nhất': {'$exists': 1}},
                         {"$unset": {'2．1 Diễn biến dư nợ 12 tháng gần nhất': 1}})
    coll_vay.update_many({'2．1 Diễn biến dư nợ 12 tháng gần nhất ': {'$exists': 1}},
                         {"$unset": {'2．1 Diễn biến dư nợ 12 tháng gần nhất ': 1}})
    coll_vay.update_many({'2．5． Diễn biến dư nợ 12 tháng gần nhất': {'$exists': 1}},
                         {"$unset": {'2．5． Diễn biến dư nợ 12 tháng gần nhất': 1}})
    coll_vay.update_many({'2．5． Diễn biến dư nợ 12 tháng gần nhất ': {'$exists': 1}},
                         {"$unset": {'2．5． Diễn biến dư nợ 12 tháng gần nhất ': 1}})
    coll_vay.update_many({'thong_tin_nhan_dang': {'$exists': 1}},
                         {"$unset": {'thong_tin_nhan_dang': 1}})


def export_to_df(coll, key, file_path):
    # db = create_connect_to_mongo(database='cic',locahost=True)
    # query= list(db['the_tin_dung'].find())
    # coll_the = db['the_tin_dung']
    # import dataframe từ key  1_1_thong_tin_nhan_dang
    key = '1_1_thong_tin_nhan_dang'
    query = list(coll.find({}, {key: 1, '_id': 0}))
    data = [list(a.values()) for a in query]
    data1 = [a[0] for a in data if len(a) > 0]
    df = pd.DataFrame(data1)
    nam_to_excel(df, file_path)


def clean_the_tin_dung_part2():
    # clean date  trong the tin dung
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_the = db['the_tin_dung_clean']
    bulk = coll_the.initialize_ordered_bulk_op()
    batch_number = 1000
    query = coll_the.find({'thong_tin_nhan_dang.time_query': {"$exists": 1}}, {'thong_tin_nhan_dang': 1})
    counter = 0
    for doc in query:
        # a = list(query)
        # doc  = a[-3]
        # khởi tạo giá trị đầu tiên cho mỗi vòng lặp để đảm bảo k update giá trị trước của vòng lặp
        new_time = None
        mobi_phone = None
        fixed_phone = None
        time_query = doc['thong_tin_nhan_dang'].get('time_query')
        try:
            if time_query is not None:
                time_query_split = [int(a) for a in time_query.split('/') if a.isdigit()]
                new_time = datetime.datetime(time_query_split[-1], time_query_split[-2], time_query_split[0])
                # bulk.find({'_id':doc['_id']}).update_one({"$set":{'thong_tin_nhan_dang.time_query':new_time}})
            phone_raw = doc['thong_tin_nhan_dang'].get('phone_number')
            # phone_raw = '0915400120 0437472828'
            if phone_raw is not None:
                dict_phone = apply_clean_phone_number(phone_raw)
                if dict_phone is not None:
                    mobi_phone = dict_phone.get('mobile phone')
                    fixed_phone = dict_phone.get('fixed phone')
            # bulk.find({'_id':doc['_id']}).update_one({"$set":{
            #     'thong_tin_nhan_dang.time_query_correct':new_time,
            #     'thong_tin_nhan_dang.phone.mobi_phone':mobi_phone,
            #     'thong_tin_nhan_dang.phone.fixed_phone':fixed_phone}})
            if new_time is not None:
                bulk.find({'_id': doc['_id']}).update_one({"$set": {
                    'thong_tin_nhan_dang.time_query_correct': new_time}})
            if mobi_phone is not None:
                bulk.find({'_id': doc['_id']}).update_one({"$set": {
                    'thong_tin_nhan_dang.phone.mobi_phone': mobi_phone}})
            if fixed_phone is not None:
                bulk.find({'_id': doc['_id']}).update_one({"$set": {
                    'thong_tin_nhan_dang.phone.fixed_phone': fixed_phone}})
            counter += 1
            if counter % batch_number == 0:  # update sau khi duyet 1000 document
                try:
                    bulk.execute()
                    # print('update thanh cong')
                    bulk = coll_the.initialize_ordered_bulk_op()
                except Exception as error:
                    print(error)
        except Exception as e:
            print(e)
    if counter % batch_number != 0:  # chạy batch cho phần còn lại (mà không chia hết cho batch_number)
        try:
            bulk.execute()
            # print('update thanh cong')
        except Exception as error:
            print(error)
    correct_key_3_tong_du_no_tin_dung(coll_the)


def create_coll_customer():
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_the = db['the_tin_dung_clean']
    pipeline = [
        {"$unwind": {"path": "$thong_tin_nhan_dang.phone.mobi_phone", 'preserveNullAndEmptyArrays': True}},
        {"$unwind": {"path": "$thong_tin_nhan_dang.phone.fixed_phone", 'preserveNullAndEmptyArrays': True}},
        {"$group": {
            "_id": "$thong_tin_nhan_dang.cic_id",
            "CMND": {"$addToSet": "$thong_tin_nhan_dang.CMND"},
            "name": {"$addToSet": "$thong_tin_nhan_dang.name"},
            "address": {"$addToSet": "$thong_tin_nhan_dang.address"},
            # "mobi_phone": {"$addToSet": {"$each":"$thong_tin_nhan_dang.phone.mobi_phone"}},
            # "fixed_phone": {"$addToSet":{"$each":"$thong_tin_nhan_dang.phone.fixed_phone"}},
            "mobi_phone": {"$addToSet": "$thong_tin_nhan_dang.phone.mobi_phone"},
            "fixed_phone": {"$addToSet": "$thong_tin_nhan_dang.phone.fixed_phone"},
            "header": {"$addToSet": "$thong_tin_nhan_dang.header"},
            "time_query": {"$addToSet": "$thong_tin_nhan_dang.time_query"},
        }},
        {"$out": 'customer'}
    ]
    coll_the.aggregate(pipeline, allowDiskUse=True)
    # field_12 = check_unique_key(coll_the, )


def clean_the_tin_dung_part1():
    # clean database the tin dung
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_the = db['the_tin_dung_clean']

    coll_the.update_many({},
                         {'$rename': {'1．2． Thông tin về tổ chức phát hành thẻ ': 'thong_tin_to_chuc_phat_hanh_the'}})
    coll_the.update_many({}, {'$rename': {'1．3． Thông tin tài sản đảm bảo': 'thong_tin_tsdb'}})
    coll_the.update_many({}, {
        '$rename': {'2．1． Thông tin về số tiền thanh toán thẻ của chủ thẻ ': 'thong_tin_so_tien_thanh_toan_chu_the'}})
    coll_the.update_many({}, {'$rename': {'2．2．2． Từ 15/8/2013 đến nay ': 'lich_su_cham_thanh_toan_sau_2013'}})
    coll_the.update_many({}, {'$rename': {'2．3 Tình hình thanh toán thẻ của chủ thẻ ': 'tinh_hinh_cham_thanh_toan'}})

    # xóa column ''tinh_hinh_cham_thanh_toan''
    coll_the.update_many({'tinh_hinh_cham_thanh_toan': {'$exists': 1}}, {"$unset": {'tinh_hinh_cham_thanh_toan': 1}})


def statistic_the_tin_dung():
    # lich_su_cham_thanh_toan không
    ''' var all_cus = db.getCollection('customer').find({}).count() //151242 KH
    var cus_have_pphone = db.getCollection('customer').find({'mobi_phone.0':{$exists:1}}).count() //49329 KH
    // db.getCollection('customer').find({}).count() //151243 KH
    var number_duplicate_cic_id =   db.getCollection('customer').find({'CMND.1':{$exists:1},'name.1':{$exists:1}}).count() // check number customer have two CMND and two name different  (1 cic_id có nhiều hơn 2 CMND và tên
    db.the_tin_dung_clean.aggregate([
    {
        $match:{ 
            'thong_tin_to_chuc_phat_hanh_the.0':{$exists:1},
            'thong_tin_nhan_dang.phone.mobi_phone.0':{$exists:1}
            },

            },
    {
        $group:{
            '_id':"$thong_tin_nhan_dang.cic_id",
            }},
    {
        $group:{
            '_id': 1,
            count:{$sum:1}}}
            ]) //29040 số khách hàng có thẻ và có số điện thoại 


db.the_tin_dung_clean.aggregate([
{
    $match:{ 
        'thong_tin_to_chuc_phat_hanh_the.0':{$exists:1},
        'thong_tin_nhan_dang.phone.mobi_phone.0':{$exists:1},
        $or:[{'lich_su_cham_thanh_toan.0':{$exists:1}},
        {'lich_su_cham_thanh_toan_sau_2013.0':{$exists:1}},
        {'lich_su_cham_thanh_toan_truoc_2013.0':{$exists:1}}]
        },

        },
{
    $group:{
        '_id':"$thong_tin_nhan_dang.cic_id",
        }},
{
    $group:{
        '_id': 1,
        count:{$sum:1}}}
        ])  //4375 Kh đã từng chậm thanh toán 
        '''


#  thông tin nợ xấu:    lich_su_no_xau_3_nam , no_can_chu_y_12_thang field nợ xấu

# thông tin dư nợ :  tong_du_no_tin_dung

def clean_vay_part2():
    db = create_connect_to_mongo(database='cic',locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_the = db['vay_clean']
    bulk = coll_the.initialize_ordered_bulk_op()
    batch_number = 1000
    query = coll_the.find({'1_thong_tin_nhan_dang.time_query':{"$exists":1}})
    counter = 0
    for doc in query:
        # a = list(query)
        # doc  = a[-3]
        # khởi tạo giá trị đầu tiên cho mỗi vòng lặp để đảm bảo k update giá trị trước của vòng lặp
        new_time =None
        time_query = doc['1_thong_tin_nhan_dang'].get('time_query')
        # time_query = '06/04/2015'
        try:
            if time_query is not None:
                time_query = time_query.split('-')[-1]
                time_query_split = [int(a)  if a.isdigit() else int(a[-2:]) for a in time_query.split('/')]
                new_time = datetime.datetime(time_query_split[-1],time_query_split[-2],time_query_split[0])
                # bulk.find({'_id':doc['_id']}).update_one({"$set":{'thong_tin_nhan_dang.time_query':new_time}})
            if new_time is not None:
                bulk.find({'_id':doc['_id']}).update_one({"$set":{
                    '1_thong_tin_nhan_dang.time_query_correct':new_time}})
            counter +=1
            if counter % batch_number == 0:  # update sau khi duyet 1000 document
                try:
                    bulk.execute()
                    # print('update thanh cong')
                    bulk = coll_the.initialize_ordered_bulk_op()
                except Exception as error:
                    print(error)
        except Exception as e:
            print(e)
    if counter % batch_number != 0:  # chạy batch cho phần còn lại (mà không chia hết cho batch_number)
        try:
            bulk.execute()
            # print('update thanh cong')
        except Exception as error:
            print(error)
if __name__ == '__main__':
    # nam_2308()
    # nam_25_08()
    # clean_the_tin_dung_part1()
    # clean_the_tin_dung_part2()
    # create_coll_customer()
    #
    # db = create_connect_to_mongo(database='cic',locahost=True)
    # # query= list(db['the_tin_dung'].find())
    # coll_the = db['the_tin_dung_clean']
    # field_12 = check_unique_key(coll_the, )
    # #
    # clean_the_tin_dung_part1()
    # clean_vay_part1()
    clean_vay_part2()
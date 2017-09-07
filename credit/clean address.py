import googlemaps
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import  re
import pandas as pd
from nam_basic import  create_connect_to_mongo, split_list_to_N_equal_element, NamError
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from multiprocessing import Process, Value, Array, Pool,freeze_support, Lock, Queue
from selenium.common.exceptions import UnexpectedAlertPresentException, WebDriverException


def call_api_address(list_id,lock,coll_cus_name):
    '''
    ys tưởng : chạy multiprocess, và nếu bị lỗi gì, thì ta gọi lại chính funtion này, nhưng kiểm tra trong list lại những id_ đã được update hay chưa, nếu 
    :param list_id: 
    :param lock: 
    :param coll_cus_name: 
    :return: 
    '''
    db = create_connect_to_mongo(database='cic', locahost=True)
    # phải tạo được connection trong function để có thể update được
    coll_cus_name = 'customer'
    coll_cus = db[coll_cus_name]
    driver = webdriver.Firefox()
    driver.get('http://vi.mygeoposition.com/?lang=vi')
    time.sleep(1)
    # list_id = [a['_id'] for a in query]
    query_true = list(coll_cus.find({'address_new':{'$exists':0},'_id':{'$in': list_id}},{'address':1}))

    # query_true = list(set(query_all).intersection(set(query)))
    try:
        for doc in query_true:
            try:
                input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "query")))
            except (UnexpectedAlertPresentException):
                driver.switch_to.alert.accept()
            # except WebDriverException:
            #     print('WebDriverException')
            except Exception as e:
                print(e)
                driver.close()
                driver = webdriver.Firefox()
                driver.get('http://vi.mygeoposition.com/?lang=vi')
                time.sleep(1)
                continue
            input_vaues = doc['address']
            list_address = []
            for input_vaue in input_vaues:
                # input_vaue = 'dfdfdfdfdfd'
                try:
                    address = None
                    vi_do = None
                    kinh_do = None
                    khu_vuc_quan_ly = None
                    quan_huyen = None
                    khu_vuc_quan_ly_nho = None
                    vi_tri = None
                    duong_chinh = None
                    duong_chinh_khac = None
                    input.clear()
                    input.send_keys(input_vaue, Keys.RETURN)
                    # time.sleep(1)
                    # chọn form geodata
                    try:  # nếu có alert thì click chọn , ta chỉ nên đợi 3s thôi
                        geo_form = driver.find_element_by_xpath("//ul/li[3]/a")
                        time.sleep(0.5)
                    except UnexpectedAlertPresentException: #neu co loi thi k can phai doi
                        # WebDriverWait(driver,5).until(EC.alert_is_present())
                        driver.switch_to.alert.accept()
                        continue
                    except:
                        driver.close()
                        driver = webdriver.Firefox()
                        driver.get('http://vi.mygeoposition.com/?lang=vi')
                        continue

                    geo_form.click()
                    source = driver.page_source
                    soup = BeautifulSoup(source, 'html.parser')
                    try:
                        select_form = driver.find_element_by_xpath("//p/button")
                        select_form.click()
                    except:
                        pass
                    df = pd.read_html(source)[0]
                    address = df[df[0] == 'Địa chỉ:'][1].values[0]
                    address = None if pd.isnull(address) else address
                    vi_do = df[df[0] == 'Vĩ độ:'][1].values[0]
                    vi_do = None if pd.isnull(vi_do) else vi_do

                    kinh_do = df[df[0] == 'Kinh độ:'][1].values[0]
                    kinh_do = None if pd.isnull(kinh_do) else kinh_do

                    khu_vuc_quan_ly = df[df[0] == 'Khu vực quản lý:'][1].values[0]
                    khu_vuc_quan_ly = None if pd.isnull(khu_vuc_quan_ly) else khu_vuc_quan_ly

                    quan_huyen = df[df[0] == 'Quận:'][1].values[0]
                    quan_huyen = None if pd.isnull(quan_huyen) else quan_huyen

                    khu_vuc_quan_ly_nho = df[df[0] == 'Khu vực quản lý cấp dưới:'][1].values[0]
                    khu_vuc_quan_ly_nho = None if pd.isnull(khu_vuc_quan_ly_nho) else khu_vuc_quan_ly_nho

                    vi_tri = df[df[0] == 'Vị trí:'][1].values[0]
                    vi_tri = None if pd.isnull(vi_tri) else vi_tri

                    duong_chinh = df[df[0] == 'Đường chính:'][1].values[0]
                    duong_chinh = None if pd.isnull(duong_chinh) else duong_chinh

                    duong_chinh_khac = df[df[0] == 'Đường chính #:'][1].values[0]
                    duong_chinh_khac = None if pd.isnull(duong_chinh_khac) else duong_chinh_khac

                    if address == 'Hà Nội, Hoàn Kiếm, Hà Nội, Việt Nam':
                        address = None
                        vi_do = None
                        kinh_do = None
                        khu_vuc_quan_ly = None
                        quan_huyen = None
                        khu_vuc_quan_ly_nho = None
                        vi_tri = None
                        duong_chinh = None
                        duong_chinh_khac = None
                    # all = df.T.to_json()
                    # dict_new = {'address':address,'all':all}
                    dict_new = {'address': address,
                                'vi_do': vi_do,
                                'kinh_do': kinh_do,
                                'khu_vuc_quan_ly': khu_vuc_quan_ly,
                                'quan_huyen': quan_huyen,
                                'khu_vuc_quan_ly_nho': khu_vuc_quan_ly_nho,
                                'vi_tri': vi_tri,
                                'duong_chinh': duong_chinh,
                                'duong_chinh_khac': duong_chinh_khac,
                                }
                    list_address.append(dict_new)
                except (UnexpectedAlertPresentException) :
                    # WebDriverWait(driver, 5).until(EC.alert_is_present())
                    driver.switch_to.alert.accept()
                # except WebDriverException:
                #     print('WebDriverException')
                except Exception as e:
                    print(e)
                    print('loi tai adress: {}'.format(input_vaues))
                    driver.close()
                    driver = webdriver.Firefox()
                    driver.get('http://vi.mygeoposition.com/?lang=vi')
                    time.sleep(1)
            with lock:
                coll_cus.update_one(
                    {'_id': doc['_id']},
                    {'$set': {'address_new': list_address}})
    except:
        # driver.close()
        call_api_address(list_id, lock, coll_cus_name)
def run_call_api():
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_cus = db['customer']
    # coll_cus.update_many({},{'$unset': {'address_new':1}})
    # query những document mà không có trường address_new (chú ý nếu cần thì có thể query những thằng có address_new.address = null)
    query = list(coll_cus.find({'address_new':{'$exists':0}},{'address':1}))
    list_id = [a['_id'] for a in query]
    coll_cus_name = 'customer'
    lock = Lock()
    processes = [Process(target=call_api_address, args=(list_file,lock,coll_cus_name)) for i,list_file in enumerate(split_list_to_N_equal_element(list_id,6))]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()


if __name__ == '__main__':
    run_call_api()
    # call_api_address1()
#
#
# def add_string(a, b):
#     a = '' if pd.isnull(a) else str(a)
#     b = '' if pd.isnull(b) else str(b)
#     return a + ', ' + b
#
#
# def find_table_containt_string(soup, string):
#     # string = 'Số'
#     next_tag = soup.find(text=re.compile(string)).find_parent('table')
#     return next_tag
#
#
# def find_table_between_index(soup, index_next, index_previous=None):
#     # tim the 1.2, sau do tim table tag nam sau the nay.
#     # ý tưởng, tìm những thẻ table đằng sau nằm giữa 2 thẻ chứa 1 chỉ mục nào đó, ví dụ nằm giữa 1.2 và 1.3
#     # thực ra có thể tự tìm các pattern 1.2, 1.3 ,... sau đó chạy vòng lặp theo thứ tự
#
#     # neu không tìm ra table thì sẽ tag sẽ là thẻ rỗng
#
#     # index_next = '^3\.'
#     # index_previous = '^4\.'
#     #kiểm tra nếu có nhiều mẫu thỏa mãn, thì sẽ log lại
#     # if len(soup.find_all(text=re.compile(index_next),attrs = {'class':re.compile('.')})) >1:
#     #     logger.warning('co 2 gia tri text thao mãn pattern %s' %(index_next))
#     next_tag = set(soup.find(text=re.compile(index_next),attrs = {'class':re.compile('.')}).find_all_next('table')) if soup.find(
#         text=re.compile(index_next)) is not None else set()
#
#     if index_previous is not None:
#         # if len(soup.find_all(text=re.compile(index_previous))) > 1:
#         #     logger.warning('co 2 gia tri text thao mãn pattern %s' % (index_previous))
#         previous_tag = set(soup.find(text=re.compile(index_previous),attrs = {'class':re.compile('.')}).find_all_previous('table')) if soup.find(
#             text=re.compile(index_previous)) is not None else set()
#         tag = next_tag.intersection(previous_tag)
#         # list(tag)[0]
#     else:
#         tag = next_tag
#     if len(tag) > 1:
#         # raise ValueError('tìm được 2 bảng ở đây.')
#         print('tìm được 2 bảng ở đây.')
#         # logger.warning('tim dc 2 bang  o day. nam giua 2 the %s vaf the %s ' %(index_next, index_previous))
#         return list(tag)[-1]
#     elif len(tag) == 1:
#         return list(tag)[0]
#     else:
#         print('không  tìm thấy table giữa 2 pattern: %s và pattern %s' %(index_next, index_previous))
#         return None
#
#
# def concatenate_tables(table1, df2):
#     # nối 2 dataframe có cùng header với nhau
#     df_concate = pd.concat([df1, df2])
#     return df_concate
#
# def import_THE_TIN_DUNG_add_more(lock, list_vay):
#
#     file_name3A = r'C:\nam\work\learn_tensorflow\credit\output\27_07_the_td\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG A.csv'
#     # driver = webdriver.Firefox()
#     directory0 = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
#
#     for filename in list_vay:
#         # try:
#             # filename = '213835.html'
#         full_filename = os.path.join(directory0,filename)
#         with open(full_filename, encoding='utf-8') as file:
#             print(filename)
#             soup = BeautifulSoup(file, "html.parser")
#         table1 = find_table_between_index(soup, '^1\.1', '^1\.2')
#         time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
#             1].get_text()
#         time_query = time_query.strip()
#
#         df_table1 = html_tables(table1)
#         # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#         df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
#         # cic_id = df_table1['Mã CIC:'].values[0]
#         cic_id = df_table1.filter(regex=(".*CIC.*")).values[0]
#
#         infor3 = soup.find(text=re.compile('^3\.'),attrs = {'class':re.compile('.')}).get_text()
#         df_table3 = pd.DataFrame([infor3],columns=['giatri'])
#
#         with lock:
#             add_infor_to_df_and_write_cvs(df_table3, file_name3A, cic_id=cic_id, time_query=time_query, filename=filename)
#         # except Exception as error:
#         #     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#         #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#
#
# def import_THE_TIN_DUNG_1808(lock, list_vay):
#     file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
#     file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
#     file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
#     file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
#     file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
#     file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
#     file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
#     file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
#     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
#
#     error_list = []
#
#     for filename in list_vay:
#         try:
#             # filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
#             with open(filename, encoding='utf-8') as file:
#                 print(filename)
#                 soup = BeautifulSoup(file, "html.parser")
#
#             table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
#             table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
#             table13 = find_table_between_index(soup, '^1\.3', '^2\.')
#             table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
#             table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
#             table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')
#
#             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
#             df_table11 = html_tables(table11)
#             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#             df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
#
#             no_number = soup.find(text=re.compile('Số:'))
#             header = soup.find('span', attrs={'class': 'headerfont'}).text
#             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
#                 1].get_text()
#             time_query = time_query.strip()
#
#
#
#             # cic_id = df_table1['Mã số CIC:'].values[0]
#             cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]
#
#             df_table12 = html_tables(table12)
#             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#             df_table12 = df_table12.clean_table( remove_na=True)
#
#             df_table21 = html_tables(table21)
#             df_table21 = df_table21.clean_table()
#
#
#             df_table21 = html_tables(table21)
#             df_table21 = df_table21.clean_table()
#
#             df_table22a = html_tables(table22a)
#             df_table22a = df_table22a.clean_table()
#             try: # thi thoảng ta k có table 22b
#                 df_table22b = html_tables(table22b)
#                 df_table22b = df_table22a.clean_table()
#             except:
#                 df_table22b = None
#             infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
#             df_table3 = pd.DataFrame([infor3], columns=['giatri'])
#             #
#             # add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
#             #                               time_query=time_query, filename=filename)
#             # add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
#             # add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
#             # add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
#
#
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
#                                               time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
#         except Exception as error:
#             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#
#
# def import_THE_TIN_DUNG_1808_single(lock, list_vay):
#     '''
#     function cho chay từng file riêng lẻ
#     :param lock:
#     :param list_vay:
#     :return:
#     '''
#     file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
#     file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
#     file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
#     file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
#     file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
#     file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
#     file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
#     file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
#     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
#
#     error_list = []
#
#     # for filename in list_vay:
#     #     try:
#     filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\100162.html'
#     with open(filename, encoding='utf-8') as file:
#         print(filename)
#         soup = BeautifulSoup(file, "html.parser")
#
#     table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
#     table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
#     table13 = find_table_between_index(soup, '^1\.3', '^2\.')
#     table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
#     table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
#     table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')
#
#     table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
#     df_table11 = html_tables(table11)
#     # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#     df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
#
#     no_number = soup.find(text=re.compile('Số:'))
#     header = soup.find('span', attrs={'class': 'headerfont'}).text
#     time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
#         1].get_text()
#     time_query = time_query.strip()
#
#
#
#     # cic_id = df_table1['Mã số CIC:'].values[0]
#     cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]
#
#     df_table12 = html_tables(table12)
#     # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#     df_table12 = df_table12.clean_table( remove_na=True)
#
#     df_table21 = html_tables(table21)
#     df_table21 = df_table21.clean_table()
#
#
#     df_table21 = html_tables(table21)
#     df_table21 = df_table21.clean_table()
#
#     df_table22a = html_tables(table22a)
#     df_table22a = df_table22a.clean_table()
#     try: # thi thoảng ta k có table 22b
#         df_table22b = html_tables(table22b)
#         df_table22b = df_table22a.clean_table()
#     except:
#         df_table22b = None
#     infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
#     df_table3 = pd.DataFrame([infor3], columns=['giatri'])
#
#     add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
#                                   time_query=time_query, filename=filename)
#     add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
#     add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
#     add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
#     add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
#     add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
#
#     #
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
#     #                                   time_query=time_query, filename=filename)
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
#     # with lock:
#     #     add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# # except Exception as error:
# #     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#
# def create_log_file(logfile='logfile'):
#     global logger
#     # logger = logging.getLogger(__name__)
#     logger.setLevel(logging.DEBUG)
#     # create file handler which logs even debug messages
#     fh = logging.FileHandler('%s.log' %(logfile))
#     fh.setLevel(logging.DEBUG)
#     # create console handler with a higher log level
#     ch = logging.StreamHandler()
#     ch.setLevel(logging.WARNING)
#     # create formatter and add it to the handlers
#     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     fh.setFormatter(formatter)
#     ch.setFormatter(formatter)
#     # add the handlers to the logger
#     logger.addHandler(fh)
#     logger.addHandler(ch)
#
# def import_BAO_CAO_VAY(lock,list_vay):
#     # global logger
#     file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
#     file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
#     file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
#     file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
#     file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
#     file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\2_5.csv'
#     file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
#     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
#
#     error_list = []
#     driver = webdriver.Firefox()
#     # print(filename_input)
#     # list_vay = [1,]
#     directory0 = r'C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/'
#
#     for filename in list_vay:
#         try:
#         # filename = '141284.html'
#         #     logger.info('bat dau ghi file %s' % (filename))
#             file_dir_for_browser = 'file:///' + directory0 + filename
#             driver.get(file_dir_for_browser)
#             source = driver.page_source
#             # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
#             # # driver.get(file_dir)
#             # source = driver.page_source
#             # voi 1 vong lap, thi k can phai quit driver.
#             soup = BeautifulSoup(source, "html.parser")
#
#             # table4 = find_table_between_index(soup, '^3\.', '^4\.')
#             table3 = find_table_between_index(soup, '^3\.', '^4\.')
#             if soup.find(text=re.compile('2\.5')) is not None:
#                 table25 = find_table_between_index(soup, '2\.5', '^3\.')
#                 table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
#             else:
#                 table25 = None
#                 table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
#             table23 = find_table_between_index(soup, '2\.3', '2\.4')
#             table22 = find_table_between_index(soup, '2\.2', '2\.3')
#             table21 = find_table_between_index(soup, '2\.1', '2\.2')
#             table1 = find_table_between_index(soup, '^1\.', '^2\.')
#             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
#
#             no_number = soup.find(text=re.compile('Số:'))
#             header = soup.find('span', attrs={'class': 'headerfont'}).text
#             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
#                 1].get_text()
#             time_query = time_query.strip()
#
#             df_table1 = html_tables(table1)
#             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#             df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
#
#             cic_id = df_table1['Mã số CIC:'].values[0]
#
#             df_table21 = html_tables(table21)
#             df_table21 = df_table21.clean_table()
#
#             df_table22 = html_tables(table22)
#             df_table22 = df_table22.clean_table()
#
#             df_table23 = html_tables(table23)
#             df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')
#
#             df_table24 = html_tables(table24)
#             df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')
#
#             # if table25 is not None:
#             df_table25 = html_tables(table25)
#             df_table25 = df_table25.clean_table()
#
#             df_table3 = html_tables(table3)
#             df_table3 = df_table3.clean_table()
#
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
#                                               time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)
#
#             # add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
#             #                                   time_query=time_query, filename=filename)
#             # add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
#             # add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query,filename=filename )
#             # add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query,filename=filename )
#             # logger.info('da ghi file thanh cong %s' % (filename))
#         except Exception as error:
#             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#
#
#
# def import_BAO_CAO_VAY_1808(lock,list_vay):
#     '''
#     function chay cho file vay đã được correct, nên chỉ cần mở file bình  thường lên và dùng beautifulsoup
#     :param lock:
#     :param list_vay:
#     :return:
#     '''
#     # global logger
#     file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
#     file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
#     file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
#     file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
#     file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
#     file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\2_5.csv'
#     file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
#     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
#
#     error_list = []
#
#     for filename in list_vay:
#         try:
#         # filename = '141284.html'
#         #     logger.info('bat dau ghi file %s' % (filename))
#         #     file_dir_for_browser = 'file:///' + directory0 + filename
#         #     driver.get(file_dir_for_browser)
#         #     source = driver.page_source
#             # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
#             # # driver.get(file_dir)
#             # source = driver.page_source
#             # voi 1 vong lap, thi k can phai quit driver.
#             with open(filename, encoding='utf-8') as file:
#                 print(filename)
#                 soup = BeautifulSoup(file, "html.parser")
#
#             # table4 = find_table_between_index(soup, '^3\.', '^4\.')
#             table3 = find_table_between_index(soup, '^3\.', '^4\.')
#             if soup.find(text=re.compile('2\.5')) is not None:
#                 table25 = find_table_between_index(soup, '2\.5', '^3\.')
#                 table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
#             else:
#                 table25 = None
#                 table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
#             table23 = find_table_between_index(soup, '2\.3', '2\.4')
#             table22 = find_table_between_index(soup, '2\.2', '2\.3')
#             table21 = find_table_between_index(soup, '2\.1', '2\.2')
#             table1 = find_table_between_index(soup, '^1\.', '^2\.')
#             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
#
#             no_number = soup.find(text=re.compile('Số:'))
#             header = soup.find('span', attrs={'class': 'headerfont'}).text
#             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
#                 1].get_text()
#             time_query = time_query.strip()
#
#             df_table1 = html_tables(table1)
#             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
#             df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
#
#             cic_id = df_table1['Mã số CIC:'].values[0]
#
#             df_table21 = html_tables(table21)
#             df_table21 = df_table21.clean_table()
#
#             df_table22 = html_tables(table22)
#             df_table22 = df_table22.clean_table()
#
#             df_table23 = html_tables(table23)
#             df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')
#
#             df_table24 = html_tables(table24)
#             df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')
#
#             # if table25 is not None:
#             df_table25 = html_tables(table25)
#             df_table25 = df_table25.clean_table()
#
#             df_table3 = html_tables(table3)
#             df_table3 = df_table3.clean_table()
#
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
#                                               time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
#             with lock:
#                 add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)
#         except Exception as error:
#             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
#
#
#
#
#
# def run_TTD1808():
#     # global logger
#     # N = len(list_vay)
#     folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
#     folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
#     folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
#     list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
#     list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
#     list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
#     list_vay = list_vay1 +list_vay2+list_vay3
#     lock = Lock()
#     processes = [Process(target=import_THE_TIN_DUNG_1808, args=(lock,files)) for files in split_list_to_N_equal_element(list_vay)]
#     # proc = processes[0]
#     # proc.start()
#     # import_THE_TIN_DUNG(lock,list_vay)
#     for proc in processes:
#         proc.start()
#     for proc in processes:
#         proc.join()
# def run_TTD():
#     # global logger
#     # N = len(list_vay)
#     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
#     list_vay = [file for file in os.listdir(folder_input)]
#     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
#     lock = Lock()
#     processes = [Process(target=import_THE_TIN_DUNG, args=(lock,file)) for file in part_list]
#     # proc = processes[0]
#     # proc.start()
#     # import_THE_TIN_DUNG(lock,list_vay)
#     for proc in processes:
#         proc.start()
#     for proc in processes:
#         proc.join()
#
# def run_TTD_add_more():
#     # global logger
#     # N = len(list_vay)
#     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
#     list_vay = [file for file in os.listdir(folder_input)]
#     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
#     lock = Lock()
#     processes = [Process(target=import_THE_TIN_DUNG_add_more, args=(lock,file)) for file in part_list]
#     # proc = processes[0]
#     # proc.start()
#     # import_THE_TIN_DUNG_add_more(lock,list_vay)
#     for proc in processes:
#         proc.start()
#     for proc in processes:
#         proc.join()
#
# def run_vay():
#     # global logger
#     # N = len(list_vay)
#     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\Vay_the_nhan'
#     list_vay = [file for file in os.listdir(folder_input)]
#     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
#     lock = Lock()
#     processes = [Process(target=import_BAO_CAO_VAY, args=(lock,file)) for file in part_list]
#     # proc = processes[0]
#     # proc.start()
#     # import_BAO_CAO_VAY(lock,list_vay)
#     for proc in processes:
#         proc.start()
#     for proc in processes:
#         proc.join()
#     # results_bank = []
# def add_infor_to_df_and_write_cvs(df, output_file, **kwargs):
#     if df is not None:
#         for key, value in kwargs.items():
#             df[key] = value
#         pd.DataFrame.sort_index(df,axis=1,inplace=True)
#         df.to_csv(output_file, encoding='utf-16', sep='\t', mode='a', index=False)
#         print('ghi file thanh cong')
#         # pd.DataFrame.to_csv()
#
# def clean_phone_number():
#     # tim dict map ma cung va tinh
#
#     dict_phone = pickle.load(open(r'C:\nam\work\learn_tensorflow\dict_phone.pickle','rb'))
#
#     # lam sach gia tri
#     df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\20.07\thong tin nhan dang.xlsx')
#     # df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\clean_sdt20_07.xlsx')
#     columns = ['co_quan', 'cic_id', 'ngay_cap', 'quoc_tich', 'cmnd', 'name', 'number', 'address']
#     df.columns = columns
#     # check lại số diện thoại bắt đầu  = +84 hoặc 84
#     df['fixed_phone'] = df['number'].apply(apply_clean_phone_number, args=('fixed phone',))
#     df['mobile_phone'] = df['number'].apply(apply_clean_phone_number, args=('mobile phone',))
#     df['other'] = df['number'].apply(apply_clean_phone_number, args=('other',))
#     df['ma_vung'] = df['fixed_phone'].apply(apply_map_pattern, args = (dict_phone,))
#
#     nam_to_excel(df, 'clean_sdt24_15h')
#
#
# class Index_doc_old():
#     '''
#     class để so sánh số thứ tự của các index
#     '''
#
#     def __init__(self, number):
#         self.number = number
#         # self.childrent = childrent
#         # self.parent = parent
#         # để compare các index với nhau, ta dùng chỉ số level, sau đó dùng để chỉ số index . VÍ dụ:  các số la mã level 0, số 1 là level 0, 1.1 là level 2
#         # order để so sánh những index cùng level với nhau.
#         self.level, self.order = self.level()
#         # __repr__ = __str__
#
#     def __str__(self):
#         return self.number
#
#     def __repr__(self):
#         return self.number
#
#     def __lt__(self, other):
#         '''
#         ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
#         :param other:
#         :return:
#         '''
#         if self.level != other.level:
#             return self.level > other.level
#             return self.level > other.level
#         else:
#             return self.order < other.order
#
#     def __gt__(self, other):
#         if self.level != other.level:
#             return self.level > other.level
#         else:
#             return self.order > other.order
#
#     def split_by(self):
#         level_regex = re.compile('[1-9]\.')
#         if re.search(level_regex, number) is not None:
#             split_number = number.split('.')
#             split_number = [x for x in split_number if x.isdigit()]
#             return split_number
#
#     def level(self):
#         '''
#         lấy self.number
#         :return:  level của index tương ứng
#         '''
#         level0_regex = re.compile('(IX\.|IV\.|V?I{1,3}\.|V\.)')
#         # level 1 la cac số từ 1-9 theo sau là dấu chấm và 1 kí tự khác số từ 1 đến 9
#         level_regex = re.compile('[1-9]\.')
#         # roman_regex = re.compile('(IX\.|IV\.|V?I{0,3}\.)')
#         number = self.number
#         # number = '1.'
#         if re.search(level0_regex, number) is not None:
#             level = 0
#             if re.search('I', number) is not None:
#                 order = 1
#             elif re.search('II', number) is not None:
#                 order = 2
#             elif re.search('III', number) is not None:
#                 order = 3
#             elif re.search('IV', number) is not None:
#                 order = 4
#             elif re.search('V', number) is not None:
#                 order = 5
#             elif re.search('VI', number) is not None:
#                 order = 6
#             elif re.search('VII', number) is not None:
#                 order = 7
#             elif re.search('VIII', number) is not None:
#                 order = 8
#             else:  # liệt kê những trường hợp khác đều là 9
#                 order = 9
#         elif re.search(level_regex, number) is not None:
#             split_number = number.split('.')
#             split_number = [x for x in split_number if x.isdigit() | re.search(level0_regex, number) is not None]
#             level = len(split_number)
#             order = int(split_number[-1])
#         else:
#             print('khong tim duoc level cua index')
#             level = 'other'
#             order = 'other'
#         return level, order
#


    # def level(self):
    #     '''
    #     lấy self.number
    #     :return:  level của index tương ứng
    #     '''
    #     level0_regex = re.compile('(IX\.|IV\.|V?I{1,3}\.|V\.)')
    #     #level 1 la cac số từ 1-9 theo sau là dấu chấm và 1 kí tự khác số từ 1 đến 9
    #     level_regex = re.compile('[1-9]\.')
    #     # roman_regex = re.compile('(IX\.|IV\.|V?I{0,3}\.)')
    #     number = self.number
    #     # number = '1.'
    #     if re.search(level0_regex ,number) is not None:
    #         level = 0
    #         if re.search('I',number) is not None:
    #             order = 1
    #         elif re.search('II',number) is not None:
    #             order = 2
    #         elif re.search('III',number) is not None:
    #             order = 3
    #         elif re.search('IV',number) is not None:
    #             order = 4
    #         elif re.search('V',number) is not None:
    #             order = 5
    #         elif re.search('VI',number) is not None:
    #             order = 6
    #         elif re.search('VII',number) is not None:
    #             order = 7
    #         elif re.search('VIII',number) is not None:
    #             order = 8
    #         else: # liệt kê những trường hợp khác đều là 9
    #             order = 9
    #     elif re.search(level_regex, number) is not None:
    #         split_number = number.split('.')
    #         split_number = [x for x in split_number if x.isdigit()|re.search(level0_regex ,number) is not None]
    #         level = len(split_number)
    #         order = int(split_number[-1])
    #     else:
    #         print('khong tim duoc level cua index')
    #         level = 'other'
    #         order = 'other'
    #     return level, order


def order_index_tag(tag):
    '''

    idea: lấy ra index của title, sau đó sắp xếp lại theo thứ tự từ nhỏ đến lớn,  kiểm tra nếu thiếu index thì thông báo, VD: ta có 1.1 , 1.3 mà không có 1.2 thì thông báo 

    :param a:
    :return: Index 
    '''

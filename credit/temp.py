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
def check_order(index_list,full_filename):
    logger = logging.getLogger(__name__)
    logger = create_log_file(logger,logfile=r'C:\nam\work\learn_tensorflow\credit\check_index',)

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
def run_check_order():
    # x = Index_doc('1.1.')
    # y = Index_doc('1.2.1')
    # print(x>y)
    logger = logging.getLogger(__name__)
    logger = create_log_file(logger,logfile=r'C:\nam\work\learn_tensorflow\credit\check_index',)
    folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
    folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
    folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
    list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
    list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
    list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
    list_vay = list_vay1 +list_vay2+list_vay3
    for full_filename in list_vay:
        # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2\366722.html'
        with open(full_filename, encoding='utf-8') as file:
            soup = BeautifulSoup(file, "html.parser")
         # lấy ra những class là bold
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
        index_tags_text = [a.get_text().strip() for a in index_tags]
        index_list_raw = [re.search(level_regex,a).group() for a in index_tags_text]
        index_list = [Index_doc(a) for a in index_list_raw]
        index_list.sort()
        a = 'check index cho list sau: {}'.format(index_list)
        logger.debug(a)
        check_order(index_list,full_filename)



from nam_basic import create_connect_to_mongo
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



def apply_clean_phone_number_2308(row,name_column):
    # row = '07103/839008 / 0909363692 071 03766669'
    # remove các kí tự không phải là số, ngoại trừ 2 trường hợp dau / va dau space dau -
    pattern_number = re.compile('[^0-9/ -]', re.IGNORECASE) # remove luôn cả chữ ext và dấu đóng mở ngoặc
    row = re.sub(pattern_number,"",str(row))
    #check truong hợp trước khi split mà được sdt thì ta sẽ ưu tiên remove  nó trước

    #thi thoang co doan can split = space, xem xet lai doan code.
    #split = dau / hoac dau space ( dau cach), dấu -
    # split_character = ['/',' ','-']
    # for character in split_character:

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



def update_field_with_regex(collection,regex,new_key):
    '''
    rename field with regex 
    :param collection: collection
    :param regex:  regex cua key cần được replace bằng new_key. VD: re.compile('nam')
    :param new_key: 
    :return: 
    '''
    #update field with regex
    # collection = db['the1']
    # new_key = '1_1_thong_tin_nhan_dang_nam_lan5'
    bulk = collection.initialize_ordered_bulk_op()
    counter = 0
    cusor = collection.find()
    for doc in cusor:
        # doc = list(cusor)[0]
        for k in doc:
            # k = '1_1_thong_tin_nhan_dang_nam_lan2'
            if re.search(regex,k) is not None:
                print('match')
                bulk.find({"_id": doc['_id']}).update_one({"$unset": {k:1}, "$set": {new_key: doc[k]}})
                counter +=1
        if counter % 1000 ==0: # update sau khi duyet 1000 document
            try:
                bulk.execute()
                bulk = collection.initialize_ordered_bulk_op()
            except Exception as error:
                print(error)
    if counter % 1000 !=0:
        try:
            bulk.execute()
        except Exception as error:
            print(error)


def nam_2308():
    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_the = db['the_tin_dung']
    # rename key : 1．1． Thông tin nhận dạng => 1_1_thong_tin_nhan_dang
    coll_the.update_many({}, {'$rename': {'1．1． Thông tin nhận dạng': '1_1_thong_tin_nhan_dang'}})
    # rename key : 1_1_thong_tin_nhan_dang.Mã số CIC: => 1_1_thong_tin_nhan_dang.Mã CIC:
    coll_the.update_many({}, {'$rename': {'1_1_thong_tin_nhan_dang.Mã số CIC:': '1_1_thong_tin_nhan_dang.Mã CIC:'}})

    # remove ban ghi của doanh nghiep
    coll_the.delete_many(
        {"1_1_thong_tin_nhan_dang.Tên doanh nghiệp:":
             {"$exists": 1}},
    )

    a = list(db['the_tin_dung'].find({}, {'1_1_thong_tin_nhan_dang.no_number': 1, '_id': 0}))
    query = list(db['the_tin_dung'].find({}, {'1_1_thong_tin_nhan_dang': 1, '_id': 0}))
    df = pd.DataFrame(a[1:2])
    df = pd.DataFrame(a[1:2])
    data = [list(a.values()) for a in query]
    data1 = [a[0] for a in data if len(a) > 0]
    # df = pd.DataFrame(data1)
    # nam_to_excel(df,r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_output\credit\output\sdt')

    # get những subkey của primary key : "1_1_thong_tin_nhan_dang"
    name_parent_key = '1_1_thong_tin_nhan_dang'
    distinctThingFields = check_unique_key(coll_the, '1_1_thong_tin_nhan_dang')
    a = [a['_id'] for a in distinctThingFields['results']]


def check_unique_key(coll):
    '''
    :param coll:  collection trong mongodb
    :return all unique key in collection: 
    '''
    mapper = Code("""
        function() {
                      for (var key in this) { emit(key, null); }
                   }
    """)
    reducer = Code("""
        function(key, stuff) { return null; }
    """)

    distinctThingFields = coll.map_reduce(mapper, reducer
        , out = {'inline' : 1}
        , full_response = True)
    ## do something with distinctThingFields['results']






def nam_pos():
    db_pos = create_connect_to_mongo(database='PosData',)
    # query= list(db['the_tin_dung'].find())
    coll_oder = db_pos['Order']
    df_order  = pd.DataFrame(list(coll_oder.find({},{'_id':0})))

    a = coll_oder.aggregate([
        {'$group':
            {'_id':'$order_id',
            'order_full': {'$addToSet': '$product_id'}}
        }
        ])
    b = coll_oder.aggregate([
        {'$group':
            {'_id':'$order_id',
            'order_full': {'$addToSet': '$name'}}
        }
        ])

    df_order1 = pd.DataFrame(list(a))
    df_order2 = pd.DataFrame(list(b))

    coll_associate = db_pos['AssociationRule']
    df_rule  = pd.DataFrame(list(coll_associate.find({},{'_id':0})))

    coll_support = db_pos['SupportRule']
    df_support  = pd.DataFrame(list(coll_support.find({},{'_id':0})))

    coll_product = db_pos['Product']
    df_product  = pd.DataFrame(list(coll_product.find({},{'_id':0})))

    support_min = 0.03
    lift = 1.5
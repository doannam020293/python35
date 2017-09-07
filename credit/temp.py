# # #
# # #
# # # def add_string(a, b):
# # #     a = '' if pd.isnull(a) else str(a)
# # #     b = '' if pd.isnull(b) else str(b)
# # #     return a + ', ' + b
# # #
# # #
# # # def find_table_containt_string(soup, string):
# # #     # string = 'Số'
# # #     next_tag = soup.find(text=re.compile(string)).find_parent('table')
# # #     return next_tag
# # #
# # #
# # # def find_table_between_index(soup, index_next, index_previous=None):
# # #     # tim the 1.2, sau do tim table tag nam sau the nay.
# # #     # ý tưởng, tìm những thẻ table đằng sau nằm giữa 2 thẻ chứa 1 chỉ mục nào đó, ví dụ nằm giữa 1.2 và 1.3
# # #     # thực ra có thể tự tìm các pattern 1.2, 1.3 ,... sau đó chạy vòng lặp theo thứ tự
# # #
# # #     # neu không tìm ra table thì sẽ tag sẽ là thẻ rỗng
# # #
# # #     # index_next = '^3\.'
# # #     # index_previous = '^4\.'
# # #     #kiểm tra nếu có nhiều mẫu thỏa mãn, thì sẽ log lại
# # #     # if len(soup.find_all(text=re.compile(index_next),attrs = {'class':re.compile('.')})) >1:
# # #     #     logger.warning('co 2 gia tri text thao mãn pattern %s' %(index_next))
# # #     next_tag = set(soup.find(text=re.compile(index_next),attrs = {'class':re.compile('.')}).find_all_next('table')) if soup.find(
# # #         text=re.compile(index_next)) is not None else set()
# # #
# # #     if index_previous is not None:
# # #         # if len(soup.find_all(text=re.compile(index_previous))) > 1:
# # #         #     logger.warning('co 2 gia tri text thao mãn pattern %s' % (index_previous))
# # #         previous_tag = set(soup.find(text=re.compile(index_previous),attrs = {'class':re.compile('.')}).find_all_previous('table')) if soup.find(
# # #             text=re.compile(index_previous)) is not None else set()
# # #         tag = next_tag.intersection(previous_tag)
# # #         # list(tag)[0]
# # #     else:
# # #         tag = next_tag
# # #     if len(tag) > 1:
# # #         # raise ValueError('tìm được 2 bảng ở đây.')
# # #         print('tìm được 2 bảng ở đây.')
# # #         # logger.warning('tim dc 2 bang  o day. nam giua 2 the %s vaf the %s ' %(index_next, index_previous))
# # #         return list(tag)[-1]
# # #     elif len(tag) == 1:
# # #         return list(tag)[0]
# # #     else:
# # #         print('không  tìm thấy table giữa 2 pattern: %s và pattern %s' %(index_next, index_previous))
# # #         return None
# # #
# # #
# # # def concatenate_tables(table1, df2):
# # #     # nối 2 dataframe có cùng header với nhau
# # #     df_concate = pd.concat([df1, df2])
# # #     return df_concate
# # #
# # # def import_THE_TIN_DUNG_add_more(lock, list_vay):
# # #
# # #     file_name3A = r'C:\nam\work\learn_tensorflow\credit\output\27_07_the_td\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG A.csv'
# # #     # driver = webdriver.Firefox()
# # #     directory0 = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
# # #
# # #     for filename in list_vay:
# # #         # try:
# # #             # filename = '213835.html'
# # #         full_filename = os.path.join(directory0,filename)
# # #         with open(full_filename, encoding='utf-8') as file:
# # #             print(filename)
# # #             soup = BeautifulSoup(file, "html.parser")
# # #         table1 = find_table_between_index(soup, '^1\.1', '^1\.2')
# # #         time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# # #             1].get_text()
# # #         time_query = time_query.strip()
# # #
# # #         df_table1 = html_tables(table1)
# # #         # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #         df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
# # #         # cic_id = df_table1['Mã CIC:'].values[0]
# # #         cic_id = df_table1.filter(regex=(".*CIC.*")).values[0]
# # #
# # #         infor3 = soup.find(text=re.compile('^3\.'),attrs = {'class':re.compile('.')}).get_text()
# # #         df_table3 = pd.DataFrame([infor3],columns=['giatri'])
# # #
# # #         with lock:
# # #             add_infor_to_df_and_write_cvs(df_table3, file_name3A, cic_id=cic_id, time_query=time_query, filename=filename)
# # #         # except Exception as error:
# # #         #     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #         #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #
# # #
# # # def import_THE_TIN_DUNG_1808(lock, list_vay):
# # #     file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
# # #     file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
# # #     file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
# # #     file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
# # #     file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
# # #     file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
# # #     file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
# # #     file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
# # #     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
# # #
# # #     error_list = []
# # #
# # #     for filename in list_vay:
# # #         try:
# # #             # filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
# # #             with open(filename, encoding='utf-8') as file:
# # #                 print(filename)
# # #                 soup = BeautifulSoup(file, "html.parser")
# # #
# # #             table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
# # #             table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
# # #             table13 = find_table_between_index(soup, '^1\.3', '^2\.')
# # #             table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
# # #             table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
# # #             table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')
# # #
# # #             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
# # #             df_table11 = html_tables(table11)
# # #             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #             df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
# # #
# # #             no_number = soup.find(text=re.compile('Số:'))
# # #             header = soup.find('span', attrs={'class': 'headerfont'}).text
# # #             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# # #                 1].get_text()
# # #             time_query = time_query.strip()
# # #
# # #
# # #
# # #             # cic_id = df_table1['Mã số CIC:'].values[0]
# # #             cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]
# # #
# # #             df_table12 = html_tables(table12)
# # #             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #             df_table12 = df_table12.clean_table( remove_na=True)
# # #
# # #             df_table21 = html_tables(table21)
# # #             df_table21 = df_table21.clean_table()
# # #
# # #
# # #             df_table21 = html_tables(table21)
# # #             df_table21 = df_table21.clean_table()
# # #
# # #             df_table22a = html_tables(table22a)
# # #             df_table22a = df_table22a.clean_table()
# # #             try: # thi thoảng ta k có table 22b
# # #                 df_table22b = html_tables(table22b)
# # #                 df_table22b = df_table22a.clean_table()
# # #             except:
# # #                 df_table22b = None
# # #             infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
# # #             df_table3 = pd.DataFrame([infor3], columns=['giatri'])
# # #             #
# # #             # add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
# # #             #                               time_query=time_query, filename=filename)
# # #             # add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             # add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             # add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# # #
# # #
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
# # #                                               time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# # #         except Exception as error:
# # #             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #
# # #
# # # def import_THE_TIN_DUNG_1808_single(lock, list_vay):
# # #     '''
# # #     function cho chay từng file riêng lẻ
# # #     :param lock:
# # #     :param list_vay:
# # #     :return:
# # #     '''
# # #     file_name11 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin nhận dạng.csv'
# # #     file_name12 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về tổ chức phát hành thẻ.csv'
# # #     file_name13 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin tài sản đảm bảo.csv'
# # #     file_name21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Thông tin về số tiền thanh toán thẻ của chủ thẻ.csv'
# # #     file_name22a = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\Lịch sử chậm thanh toán thẻ của chủ thẻ.csv'
# # #     file_name22b = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\chi tiet chậm thanh toán thẻ của chủ thẻ.csv'
# # #     file_name3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\TỔNG DƯ NỢ TÍN DỤNG CỦA CHỦ THẺ TẠI CÁC NGÂN HÀNG.csv'
# # #     file_name4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG.csv'
# # #     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\the_tin_dung\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
# # #
# # #     error_list = []
# # #
# # #     # for filename in list_vay:
# # #     #     try:
# # #     filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\100162.html'
# # #     with open(filename, encoding='utf-8') as file:
# # #         print(filename)
# # #         soup = BeautifulSoup(file, "html.parser")
# # #
# # #     table11 = find_table_between_index(soup, '^1\.1', '^1\.2')
# # #     table12 = find_table_between_index(soup, '^1\.2', '^1\.3')
# # #     table13 = find_table_between_index(soup, '^1\.3', '^2\.')
# # #     table21 = find_table_between_index(soup, '^2\.1', '^2\.2')
# # #     table22a = find_table_between_index(soup, '^2\.2', '^Chi tiết lịch sử chậm thanh toán')
# # #     table22b = find_table_between_index(soup,'Chi tiết lịch sử chậm thanh toán', '^2\.3')
# # #
# # #     table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
# # #     df_table11 = html_tables(table11)
# # #     # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #     df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
# # #
# # #     no_number = soup.find(text=re.compile('Số:'))
# # #     header = soup.find('span', attrs={'class': 'headerfont'}).text
# # #     time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# # #         1].get_text()
# # #     time_query = time_query.strip()
# # #
# # #
# # #
# # #     # cic_id = df_table1['Mã số CIC:'].values[0]
# # #     cic_id = df_table11.filter(regex=(".*CIC.*")).values[0][0]
# # #
# # #     df_table12 = html_tables(table12)
# # #     # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #     df_table12 = df_table12.clean_table( remove_na=True)
# # #
# # #     df_table21 = html_tables(table21)
# # #     df_table21 = df_table21.clean_table()
# # #
# # #
# # #     df_table21 = html_tables(table21)
# # #     df_table21 = df_table21.clean_table()
# # #
# # #     df_table22a = html_tables(table22a)
# # #     df_table22a = df_table22a.clean_table()
# # #     try: # thi thoảng ta k có table 22b
# # #         df_table22b = html_tables(table22b)
# # #         df_table22b = df_table22a.clean_table()
# # #     except:
# # #         df_table22b = None
# # #     infor3 = soup.find(text=re.compile('^3\.'), attrs={'class': re.compile('.')}).get_text()
# # #     df_table3 = pd.DataFrame([infor3], columns=['giatri'])
# # #
# # #     add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
# # #                                   time_query=time_query, filename=filename)
# # #     add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
# # #     add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
# # #     add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
# # #     add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
# # #     add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# # #
# # #     #
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table11, file_name11, header=header, no_number=no_number,
# # #     #                                   time_query=time_query, filename=filename)
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table12, file_name12, cic_id=cic_id, time_query=time_query,filename=filename )
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table21, file_name21, cic_id=cic_id, time_query=time_query, filename=filename)
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table22a, file_name22a, cic_id=cic_id, time_query=time_query,filename=filename )
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table22b, file_name22b, cic_id=cic_id, time_query=time_query, filename=filename)
# # #     # with lock:
# # #     #     add_infor_to_df_and_write_cvs(df_table3, file_name3, cic_id=cic_id, time_query=time_query,filename=filename )
# # # # except Exception as error:
# # # #     # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # # #     print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #
# # # def create_log_file(logfile='logfile'):
# # #     global logger
# # #     # logger = logging.getLogger(__name__)
# # #     logger.setLevel(logging.DEBUG)
# # #     # create file handler which logs even debug messages
# # #     fh = logging.FileHandler('%s.log' %(logfile))
# # #     fh.setLevel(logging.DEBUG)
# # #     # create console handler with a higher log level
# # #     ch = logging.StreamHandler()
# # #     ch.setLevel(logging.WARNING)
# # #     # create formatter and add it to the handlers
# # #     formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# # #     fh.setFormatter(formatter)
# # #     ch.setFormatter(formatter)
# # #     # add the handlers to the logger
# # #     logger.addHandler(fh)
# # #     logger.addHandler(ch)
# # #
# # # def import_BAO_CAO_VAY(lock,list_vay):
# # #     # global logger
# # #     file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
# # #     file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
# # #     file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
# # #     file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
# # #     file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
# # #     file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\2_5.csv'
# # #     file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
# # #     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\27_07_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
# # #
# # #     error_list = []
# # #     driver = webdriver.Firefox()
# # #     # print(filename_input)
# # #     # list_vay = [1,]
# # #     directory0 = r'C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/'
# # #
# # #     for filename in list_vay:
# # #         try:
# # #         # filename = '141284.html'
# # #         #     logger.info('bat dau ghi file %s' % (filename))
# # #             file_dir_for_browser = 'file:///' + directory0 + filename
# # #             driver.get(file_dir_for_browser)
# # #             source = driver.page_source
# # #             # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
# # #             # # driver.get(file_dir)
# # #             # source = driver.page_source
# # #             # voi 1 vong lap, thi k can phai quit driver.
# # #             soup = BeautifulSoup(source, "html.parser")
# # #
# # #             # table4 = find_table_between_index(soup, '^3\.', '^4\.')
# # #             table3 = find_table_between_index(soup, '^3\.', '^4\.')
# # #             if soup.find(text=re.compile('2\.5')) is not None:
# # #                 table25 = find_table_between_index(soup, '2\.5', '^3\.')
# # #                 table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
# # #             else:
# # #                 table25 = None
# # #                 table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
# # #             table23 = find_table_between_index(soup, '2\.3', '2\.4')
# # #             table22 = find_table_between_index(soup, '2\.2', '2\.3')
# # #             table21 = find_table_between_index(soup, '2\.1', '2\.2')
# # #             table1 = find_table_between_index(soup, '^1\.', '^2\.')
# # #             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
# # #
# # #             no_number = soup.find(text=re.compile('Số:'))
# # #             header = soup.find('span', attrs={'class': 'headerfont'}).text
# # #             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# # #                 1].get_text()
# # #             time_query = time_query.strip()
# # #
# # #             df_table1 = html_tables(table1)
# # #             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #             df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
# # #
# # #             cic_id = df_table1['Mã số CIC:'].values[0]
# # #
# # #             df_table21 = html_tables(table21)
# # #             df_table21 = df_table21.clean_table()
# # #
# # #             df_table22 = html_tables(table22)
# # #             df_table22 = df_table22.clean_table()
# # #
# # #             df_table23 = html_tables(table23)
# # #             df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')
# # #
# # #             df_table24 = html_tables(table24)
# # #             df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')
# # #
# # #             # if table25 is not None:
# # #             df_table25 = html_tables(table25)
# # #             df_table25 = df_table25.clean_table()
# # #
# # #             df_table3 = html_tables(table3)
# # #             df_table3 = df_table3.clean_table()
# # #
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
# # #                                               time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)
# # #
# # #             # add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
# # #             #                                   time_query=time_query, filename=filename)
# # #             # add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             # add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             # logger.info('da ghi file thanh cong %s' % (filename))
# # #         except Exception as error:
# # #             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #
# # #
# # #
# # # def import_BAO_CAO_VAY_1808(lock,list_vay):
# # #     '''
# # #     function chay cho file vay đã được correct, nên chỉ cần mở file bình  thường lên và dùng beautifulsoup
# # #     :param lock:
# # #     :param list_vay:
# # #     :return:
# # #     '''
# # #     # global logger
# # #     file_name_vay1 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN CHUNG VỀ KHÁCH HÀNG.csv'
# # #     file_name_vay21 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Diễn biến dư nợ 1 năm gần nhất.csv'
# # #     file_name_vay22 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Danh sách Tổ chức tín dụng đã từng quan hệ.csv'
# # #     file_name_vay23 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Tình trạng dư nợ tín dụng hiện tại.csv'
# # #     file_name_vay24 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\Lịch sử nợ xấu 5 năm gần nhất.csv'
# # #     file_name_vay25 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\2_5.csv'
# # #     file_name_vay3 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\DANH SÁCH TCTD TRA CỨU THÔNG TIN QUAN HỆ TÍN DỤNG CỦA KHÁCH HÀNG.csv'
# # #     file_name_vay4 = r'C:\nam\work\learn_tensorflow\credit\output\2015-2017\csv_vay\THÔNG TIN KHÁC VỀ KHÁCH HÀNG VAY.csv'
# # #
# # #     error_list = []
# # #
# # #     for filename in list_vay:
# # #         try:
# # #         # filename = '141284.html'
# # #         #     logger.info('bat dau ghi file %s' % (filename))
# # #         #     file_dir_for_browser = 'file:///' + directory0 + filename
# # #         #     driver.get(file_dir_for_browser)
# # #         #     source = driver.page_source
# # #             # driver.get('file:///C:/nam/work/learn_tensorflow/credit/input/Vay_the_nhan/100117.html')
# # #             # # driver.get(file_dir)
# # #             # source = driver.page_source
# # #             # voi 1 vong lap, thi k can phai quit driver.
# # #             with open(filename, encoding='utf-8') as file:
# # #                 print(filename)
# # #                 soup = BeautifulSoup(file, "html.parser")
# # #
# # #             # table4 = find_table_between_index(soup, '^3\.', '^4\.')
# # #             table3 = find_table_between_index(soup, '^3\.', '^4\.')
# # #             if soup.find(text=re.compile('2\.5')) is not None:
# # #                 table25 = find_table_between_index(soup, '2\.5', '^3\.')
# # #                 table24 = find_table_between_index(soup, '2\.4', '2\.5')  # 1 vai mau bao cao cao 2.5
# # #             else:
# # #                 table25 = None
# # #                 table24 = find_table_between_index(soup, '2\.4', '^3\.')  # 1 vai mau bao cao cao 2.5
# # #             table23 = find_table_between_index(soup, '2\.3', '2\.4')
# # #             table22 = find_table_between_index(soup, '2\.2', '2\.3')
# # #             table21 = find_table_between_index(soup, '2\.1', '2\.2')
# # #             table1 = find_table_between_index(soup, '^1\.', '^2\.')
# # #             table_header = find_table_containt_string(soup, 'Ðơn vị tra cứu')
# # #
# # #             no_number = soup.find(text=re.compile('Số:'))
# # #             header = soup.find('span', attrs={'class': 'headerfont'}).text
# # #             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# # #                 1].get_text()
# # #             time_query = time_query.strip()
# # #
# # #             df_table1 = html_tables(table1)
# # #             # với table này ta sẽ remove NAN sau khi đã transpose lại  table
# # #             df_table1 = df_table1.clean_table(header='first_column', remove_na=True)
# # #
# # #             cic_id = df_table1['Mã số CIC:'].values[0]
# # #
# # #             df_table21 = html_tables(table21)
# # #             df_table21 = df_table21.clean_table()
# # #
# # #             df_table22 = html_tables(table22)
# # #             df_table22 = df_table22.clean_table()
# # #
# # #             df_table23 = html_tables(table23)
# # #             df_table23 = df_table23.clean_table(case='du_no_hien_tai_BC_VAY')
# # #
# # #             df_table24 = html_tables(table24)
# # #             df_table24 = df_table24.clean_table(case='lich_su_no_xau_BC_VAY')
# # #
# # #             # if table25 is not None:
# # #             df_table25 = html_tables(table25)
# # #             df_table25 = df_table25.clean_table()
# # #
# # #             df_table3 = html_tables(table3)
# # #             df_table3 = df_table3.clean_table()
# # #
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table1, file_name_vay1, header=header, no_number=no_number,
# # #                                               time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table21, file_name_vay21, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table22, file_name_vay22, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table23, file_name_vay23, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table24, file_name_vay24, cic_id=cic_id, time_query=time_query,filename=filename )
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table25, file_name_vay25, cic_id=cic_id, time_query=time_query, filename=filename)
# # #             with lock:
# # #                 add_infor_to_df_and_write_cvs(df_table3, file_name_vay3, cic_id=cic_id, time_query=time_query, filename=filename)
# # #         except Exception as error:
# # #             # logger.error('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(filename,error))
# # #
# # #
# # #
# # #
# # #
# # # def run_TTD1808():
# # #     # global logger
# # #     # N = len(list_vay)
# # #     folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
# # #     folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
# # #     folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
# # #     list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
# # #     list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
# # #     list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
# # #     list_vay = list_vay1 +list_vay2+list_vay3
# # #     lock = Lock()
# # #     processes = [Process(target=import_THE_TIN_DUNG_1808, args=(lock,files)) for files in split_list_to_N_equal_element(list_vay)]
# # #     # proc = processes[0]
# # #     # proc.start()
# # #     # import_THE_TIN_DUNG(lock,list_vay)
# # #     for proc in processes:
# # #         proc.start()
# # #     for proc in processes:
# # #         proc.join()
# # # def run_TTD():
# # #     # global logger
# # #     # N = len(list_vay)
# # #     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
# # #     list_vay = [file for file in os.listdir(folder_input)]
# # #     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
# # #     lock = Lock()
# # #     processes = [Process(target=import_THE_TIN_DUNG, args=(lock,file)) for file in part_list]
# # #     # proc = processes[0]
# # #     # proc.start()
# # #     # import_THE_TIN_DUNG(lock,list_vay)
# # #     for proc in processes:
# # #         proc.start()
# # #     for proc in processes:
# # #         proc.join()
# # #
# # # def run_TTD_add_more():
# # #     # global logger
# # #     # N = len(list_vay)
# # #     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\thong_tin_the'
# # #     list_vay = [file for file in os.listdir(folder_input)]
# # #     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
# # #     lock = Lock()
# # #     processes = [Process(target=import_THE_TIN_DUNG_add_more, args=(lock,file)) for file in part_list]
# # #     # proc = processes[0]
# # #     # proc.start()
# # #     # import_THE_TIN_DUNG_add_more(lock,list_vay)
# # #     for proc in processes:
# # #         proc.start()
# # #     for proc in processes:
# # #         proc.join()
# # #
# # # def run_vay():
# # #     # global logger
# # #     # N = len(list_vay)
# # #     folder_input = r'C:\nam\work\learn_tensorflow\credit\input\Vay_the_nhan'
# # #     list_vay = [file for file in os.listdir(folder_input)]
# # #     part_list = [list_vay[i:i + 10000] for i in range(0,len(list_vay),10000)]
# # #     lock = Lock()
# # #     processes = [Process(target=import_BAO_CAO_VAY, args=(lock,file)) for file in part_list]
# # #     # proc = processes[0]
# # #     # proc.start()
# # #     # import_BAO_CAO_VAY(lock,list_vay)
# # #     for proc in processes:
# # #         proc.start()
# # #     for proc in processes:
# # #         proc.join()
# # #     # results_bank = []
# # # def add_infor_to_df_and_write_cvs(df, output_file, **kwargs):
# # #     if df is not None:
# # #         for key, value in kwargs.items():
# # #             df[key] = value
# # #         pd.DataFrame.sort_index(df,axis=1,inplace=True)
# # #         df.to_csv(output_file, encoding='utf-16', sep='\t', mode='a', index=False)
# # #         print('ghi file thanh cong')
# # #         # pd.DataFrame.to_csv()
# # #
# # # def clean_phone_number():
# # #     # tim dict map ma cung va tinh
# # #
# # #     dict_phone = pickle.load(open(r'C:\nam\work\learn_tensorflow\dict_phone.pickle','rb'))
# # #
# # #     # lam sach gia tri
# # #     df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\output\20.07\thong tin nhan dang.xlsx')
# # #     # df = pd.read_excel(r'C:\nam\work\learn_tensorflow\credit\clean_sdt20_07.xlsx')
# # #     columns = ['co_quan', 'cic_id', 'ngay_cap', 'quoc_tich', 'cmnd', 'name', 'number', 'address']
# # #     df.columns = columns
# # #     # check lại số diện thoại bắt đầu  = +84 hoặc 84
# # #     df['fixed_phone'] = df['number'].apply(apply_clean_phone_number, args=('fixed phone',))
# # #     df['mobile_phone'] = df['number'].apply(apply_clean_phone_number, args=('mobile phone',))
# # #     df['other'] = df['number'].apply(apply_clean_phone_number, args=('other',))
# # #     df['ma_vung'] = df['fixed_phone'].apply(apply_map_pattern, args = (dict_phone,))
# # #
# # #     nam_to_excel(df, 'clean_sdt24_15h')
# # #
# # #
# # # class Index_doc_old():
# # #     '''
# # #     class để so sánh số thứ tự của các index
# # #     '''
# # #
# # #     def __init__(self, number):
# # #         self.number = number
# # #         # self.childrent = childrent
# # #         # self.parent = parent
# # #         # để compare các index với nhau, ta dùng chỉ số level, sau đó dùng để chỉ số index . VÍ dụ:  các số la mã level 0, số 1 là level 0, 1.1 là level 2
# # #         # order để so sánh những index cùng level với nhau.
# # #         self.level, self.order = self.level()
# # #         # __repr__ = __str__
# # #
# # #     def __str__(self):
# # #         return self.number
# # #
# # #     def __repr__(self):
# # #         return self.number
# # #
# # #     def __lt__(self, other):
# # #         '''
# # #         ta so sánh lần lượt, các chỉ số được ngăn cách bởi dấu chấm
# # #         :param other:
# # #         :return:
# # #         '''
# # #         if self.level != other.level:
# # #             return self.level > other.level
# # #             return self.level > other.level
# # #         else:
# # #             return self.order < other.order
# # #
# # #     def __gt__(self, other):
# # #         if self.level != other.level:
# # #             return self.level > other.level
# # #         else:
# # #             return self.order > other.order
# # #
# # #     def split_by(self):
# # #         level_regex = re.compile('[1-9]\.')
# # #         if re.search(level_regex, number) is not None:
# # #             split_number = number.split('.')
# # #             split_number = [x for x in split_number if x.isdigit()]
# # #             return split_number
# # #
# # #     def level(self):
# # #         '''
# # #         lấy self.number
# # #         :return:  level của index tương ứng
# # #         '''
# # #         level0_regex = re.compile('(IX\.|IV\.|V?I{1,3}\.|V\.)')
# # #         # level 1 la cac số từ 1-9 theo sau là dấu chấm và 1 kí tự khác số từ 1 đến 9
# # #         level_regex = re.compile('[1-9]\.')
# # #         # roman_regex = re.compile('(IX\.|IV\.|V?I{0,3}\.)')
# # #         number = self.number
# # #         # number = '1.'
# # #         if re.search(level0_regex, number) is not None:
# # #             level = 0
# # #             if re.search('I', number) is not None:
# # #                 order = 1
# # #             elif re.search('II', number) is not None:
# # #                 order = 2
# # #             elif re.search('III', number) is not None:
# # #                 order = 3
# # #             elif re.search('IV', number) is not None:
# # #                 order = 4
# # #             elif re.search('V', number) is not None:
# # #                 order = 5
# # #             elif re.search('VI', number) is not None:
# # #                 order = 6
# # #             elif re.search('VII', number) is not None:
# # #                 order = 7
# # #             elif re.search('VIII', number) is not None:
# # #                 order = 8
# # #             else:  # liệt kê những trường hợp khác đều là 9
# # #                 order = 9
# # #         elif re.search(level_regex, number) is not None:
# # #             split_number = number.split('.')
# # #             split_number = [x for x in split_number if x.isdigit() | re.search(level0_regex, number) is not None]
# # #             level = len(split_number)
# # #             order = int(split_number[-1])
# # #         else:
# # #             print('khong tim duoc level cua index')
# # #             level = 'other'
# # #             order = 'other'
# # #         return level, order
# # #
# #
# #
# #     # def level(self):
# #     #     '''
# #     #     lấy self.number
# #     #     :return:  level của index tương ứng
# #     #     '''
# #     #     level0_regex = re.compile('(IX\.|IV\.|V?I{1,3}\.|V\.)')
# #     #     #level 1 la cac số từ 1-9 theo sau là dấu chấm và 1 kí tự khác số từ 1 đến 9
# #     #     level_regex = re.compile('[1-9]\.')
# #     #     # roman_regex = re.compile('(IX\.|IV\.|V?I{0,3}\.)')
# #     #     number = self.number
# #     #     # number = '1.'
# #     #     if re.search(level0_regex ,number) is not None:
# #     #         level = 0
# #     #         if re.search('I',number) is not None:
# #     #             order = 1
# #     #         elif re.search('II',number) is not None:
# #     #             order = 2
# #     #         elif re.search('III',number) is not None:
# #     #             order = 3
# #     #         elif re.search('IV',number) is not None:
# #     #             order = 4
# #     #         elif re.search('V',number) is not None:
# #     #             order = 5
# #     #         elif re.search('VI',number) is not None:
# #     #             order = 6
# #     #         elif re.search('VII',number) is not None:
# #     #             order = 7
# #     #         elif re.search('VIII',number) is not None:
# #     #             order = 8
# #     #         else: # liệt kê những trường hợp khác đều là 9
# #     #             order = 9
# #     #     elif re.search(level_regex, number) is not None:
# #     #         split_number = number.split('.')
# #     #         split_number = [x for x in split_number if x.isdigit()|re.search(level0_regex ,number) is not None]
# #     #         level = len(split_number)
# #     #         order = int(split_number[-1])
# #     #     else:
# #     #         print('khong tim duoc level cua index')
# #     #         level = 'other'
# #     #         order = 'other'
# #     #     return level, order
# #
# #
# # def order_index_tag(tag):
# #     '''
# #
# #     idea: lấy ra index của title, sau đó sắp xếp lại theo thứ tự từ nhỏ đến lớn,  kiểm tra nếu thiếu index thì thông báo, VD: ta có 1.1 , 1.3 mà không có 1.2 thì thông báo
# #
# #     :param a:
# #     :return: Index
# #     '''
# # def check_order(index_list,full_filename):
# #     logger = logging.getLogger(__name__)
# #     logger = create_log_file(logger,logfile=r'C:\nam\work\learn_tensorflow\credit\check_index',)
# #
# #     index_tags_text = [a.get_text().strip() for a in index_tags]
# #     index_list_raw = [re.search(level_regex, a).group() for a in index_tags_text]
# #     index_list = [Index_doc(a) for a in index_list_raw]
# #     index_list.sort()
# #     a = 'check index cho list sau: {}'.format(index_list)
# #     logger.debug(a)
# #     index_list.sort()
# #     for i in range(len(index_list)):
# #         if i >1:
# #             check = (index_list[i] -index_list[i-1])
# #             if check !=1:
# #                 a  = 'thieu index giua 2 index sau: {} va {} cua file name: {}'.format(index_list[i],index_list[i-1],full_filename)
# #                 logger.debug(a)
# #                 print(a)
# #             else:
# #                 logger.debug('chuan')
# # def run_check_order():
# #     # x = Index_doc('1.1.')
# #     # y = Index_doc('1.2.1')
# #     # print(x>y)
# #     logger = logging.getLogger(__name__)
# #     logger = create_log_file(logger,logfile=r'C:\nam\work\learn_tensorflow\credit\check_index',)
# #     folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
# #     folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
# #     folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
# #     list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
# #     list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
# #     list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
# #     list_vay = list_vay1 +list_vay2+list_vay3
# #     for full_filename in list_vay:
# #         # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2\366722.html'
# #         with open(full_filename, encoding='utf-8') as file:
# #             soup = BeautifulSoup(file, "html.parser")
# #          # lấy ra những class là bold
# #         style_scc =soup.style
# #         list_css = (style_scc.get_text()).split('.')
# #         list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
# #         # tạo regex những class là bold
# #         regex_bold_class = re.compile('|'.join(list_bold_tag))
# #         # tìm structure của file
# #         # regex kiểu 1.1, 1.2, II, I, V,...
# #         index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
# #         level_regex = re.compile('[1-9]\.[1-9]*')
# #
# #         index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
# #         index_tags_text = [a.get_text().strip() for a in index_tags]
# #         index_list_raw = [re.search(level_regex,a).group() for a in index_tags_text]
# #         index_list = [Index_doc(a) for a in index_list_raw]
# #         index_list.sort()
# #         a = 'check index cho list sau: {}'.format(index_list)
# #         logger.debug(a)
# #         check_order(index_list,full_filename)
# #
# #
# #
# # from nam_basic import create_connect_to_mongo
# # def import_Vay_to_mongodb(list_file,lock):
# #     #connect mongo
# #     db_cic = create_connect_to_mongo(locahost=True,database='cic')
# #     coll_the = db_cic['vay']
# #     # find structure. TÌm nhuengx header lơn của file
# #     for full_filename in list_file:
# #         try:
# #             input_db = dict()
# #             # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
# #             with open(full_filename, encoding='utf-8') as file:
# #                 soup = BeautifulSoup(file, "html.parser")
# #
# #             #thông tin k nằm trong table nào
# #             no_number = soup.find(text=re.compile('Số:'))
# #             header = soup.find('span', attrs={'class': 'headerfont'}).text
# #             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# #                 1].get_text()
# #             time_query = time_query.strip()
# #
# #
# #              # lấy ra những class là bold
# #             style_scc =soup.style
# #             list_css = (style_scc.get_text()).split('.')
# #             list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
# #             # tạo regex những class là bold
# #             regex_bold_class = re.compile('|'.join(list_bold_tag))
# #             # tìm structure của file
# #             # regex kiểu 1.1, 1.2, II, I, V,...
# #             index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
# #             index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
# #             # index_tags_text = [a.get_text().strip() for a in index_tags]
# #
# #             regex_thong_tin_nhan_dang = re.compile("thông tin nhận dạng|1\.1")
# #             regex_table12_card = re.compile('1\.2')
# #             for i in range(len(index_tags)):
# #                 # i = 0
# #                 record = dict()
# #                 first_tag = index_tags[i]
# #                 next_tag = set(first_tag.find_all_next('table'))
# #                 if i < len(index_tags)-1:
# #                     second_tag = index_tags[i+1]
# #                     previous_tag = set(second_tag.find_all_previous('table'))
# #                     tag = next_tag.intersection(previous_tag)
# #                 else:
# #                     tag = next_tag
# #                 key = first_tag.get_text()
# #                 # kiểm tra xem có bao nhiêu table giữa các index
# #                 if len(tag) ==0:
# #                     # nếu k có table thì ta thử tìm text giữ 2
# #                     next_tag = set(first_tag.find_all_next())
# #                     previous_tag = set(second_tag.find_all_previous())
# #                     tag = next_tag.intersection(previous_tag)
# #                     text = [x.get_text() for x in tag]
# #                     record = ' '.join(text).strip()
# #                 elif len(tag) ==1:
# #                     df_table11 = html_tables(list(tag)[0])
# #                     # từng loại table sẽ có cách lấy table khác nhau
# #                     if re.search(regex_thong_tin_nhan_dang,key) is not None:
# #                         # day là table 1.1
# #                         df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
# #                         #table chỉ có 1 dòng, nên ta sẽ lấy giá trị đầu tiên của nó
# #                         record = df_table11.to_dict('records')[0]
# #                         record['no_number'] = no_number
# #                         record['header'] =header
# #                         record['time_query'] = time_query
# #                         record['full_filename'] = full_filename
# #                     elif re.search(regex_table12_card,key) is not None:
# #                         # với table 12 này ta sẽ remove NAN sau khi đã transpose lại  table
# #                         df_table11 = df_table11.clean_table( remove_na=True)
# #                         record = df_table11.to_dict('records')
# #                     else:
# #                         df_table11 = df_table11.clean_table()
# #                         record = df_table11.to_dict('records')
# #                     # record[key] = record
# #                 else:
# #                     print('nhieu table tai day')
# #                 # nếu dữ liệu thu được giữa cac index khác none thì ghi vào db
# #                 if record != '':
# #                     key = key.replace('.', '\uff0E')
# #                     input_db[key] = record
# #                     input_db['_id'] = full_filename
# #             with lock:
# #                 # print(full_filename)
# #                 # coll_the = db_cic['the_tin_dung']
# #                 coll_the.insert_one(input_db)
# #         except Exception as error:
# #             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(full_filename,error))
# #
# #
# # def import_The_Tin_dung_to_mongodb(list_file,lock):
# #     #connect mongo
# #     db_cic = create_connect_to_mongo(locahost=True,database='cic')
# #     coll_the = db_cic['the_tin_dung']
# #     # find structure. TÌm nhuengx header lơn của file
# #     for full_filename in list_file:
# #         try:
# #             input_db = dict()
# #             # full_filename = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0\94.html'
# #             with open(full_filename, encoding='utf-8') as file:
# #                 soup = BeautifulSoup(file, "html.parser")
# #
# #             #thông tin k nằm trong table nào
# #             no_number = soup.find(text=re.compile('Số:'))
# #             header = soup.find('span', attrs={'class': 'headerfont'}).text
# #             time_query = list(soup.find(text=re.compile('Thời gian gửi báo cáo:')).parent.parent.next_siblings)[
# #                 1].get_text()
# #             time_query = time_query.strip()
# #
# #
# #              # lấy ra những class là bold
# #             style_scc =soup.style
# #             list_css = (style_scc.get_text()).split('.')
# #             list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
# #             # tạo regex những class là bold
# #             regex_bold_class = re.compile('|'.join(list_bold_tag))
# #             # tìm structure của file
# #             # regex kiểu 1.1, 1.2, II, I, V,...
# #             index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
# #             index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
# #             # index_tags_text = [a.get_text().strip() for a in index_tags]
# #
# #             regex_thong_tin_nhan_dang = re.compile("thông tin nhận dạng|1\.1")
# #             regex_table12_card = re.compile('1\.2')
# #             for i in range(len(index_tags)):
# #                 # i = 0
# #                 record = dict()
# #                 first_tag = index_tags[i]
# #                 next_tag = set(first_tag.find_all_next('table'))
# #                 if i < len(index_tags)-1:
# #                     second_tag = index_tags[i+1]
# #                     previous_tag = set(second_tag.find_all_previous('table'))
# #                     tag = next_tag.intersection(previous_tag)
# #                 else:
# #                     tag = next_tag
# #                 key = first_tag.get_text()
# #                 # kiểm tra xem có bao nhiêu table giữa các index
# #                 if len(tag) ==0:
# #                     # nếu k có table thì ta thử tìm text giữ 2
# #                     next_tag = set(first_tag.find_all_next())
# #                     previous_tag = set(second_tag.find_all_previous())
# #                     tag = next_tag.intersection(previous_tag)
# #                     text = [x.get_text() for x in tag]
# #                     record = ' '.join(text).strip()
# #                 elif len(tag) ==1:
# #                     df_table11 = html_tables(list(tag)[0])
# #                     # từng loại table sẽ có cách lấy table khác nhau
# #                     if re.search(regex_thong_tin_nhan_dang,key) is not None:
# #                         # day là table 1.1
# #                         df_table11 = df_table11.clean_table(header='first_column', remove_na=True)
# #                         #table chỉ có 1 dòng, nên ta sẽ lấy giá trị đầu tiên của nó
# #                         record = df_table11.to_dict('records')[0]
# #                         record['no_number'] = no_number
# #                         record['header'] =header
# #                         record['time_query'] = time_query
# #                         record['full_filename'] = full_filename
# #                     elif re.search(regex_table12_card,key) is not None:
# #                         # với table 12 này ta sẽ remove NAN sau khi đã transpose lại  table
# #                         df_table11 = df_table11.clean_table( remove_na=True)
# #                         record = df_table11.to_dict('records')
# #                     else:
# #                         df_table11 = df_table11.clean_table()
# #                         record = df_table11.to_dict('records')
# #                     # record[key] = record
# #                 else:
# #                     print('nhieu table tai day')
# #                 # nếu dữ liệu thu được giữa cac index khác none thì ghi vào db
# #                 if record != '':
# #                     key = key.replace('.', '\uff0E')
# #                     input_db[key] = record
# #                     input_db['_id'] = full_filename
# #             with lock:
# #                 # print(full_filename)
# #                 # coll_the = db_cic['the_tin_dung']
# #                 coll_the.insert_one(input_db)
# #         except Exception as error:
# #             print('error at file: (%s), chi tiet loi nhu sau: (%s)' %(full_filename,error))
# #
# # def run_TTD2108():
# #
# #     folder_input1 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\0'
# #     folder_input2 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\2'
# #     folder_input3 = r'C:\nam\work\learn_tensorflow\credit\output\pickle\8'
# #     list_vay1 = [os.path.join(folder_input1,file) for file in os.listdir(folder_input1)]
# #     list_vay2 = [os.path.join(folder_input2,file) for file in os.listdir(folder_input2)]
# #     list_vay3 = [os.path.join(folder_input3,file) for file in os.listdir(folder_input3)]
# #     list_vay = list_vay1 +list_vay2+list_vay3
# #     # num = Value('i', 0)
# #     lock = Lock()
# #     #chia list_vay thành 6 phần. bằng function được định nghĩa trong nam_basic
# #     processes = [Process(target=import_The_Tin_dung_to_mongodb, args=(list_file,lock)) for list_file in split_list_to_N_equal_element(list_vay,6)]
# #     for proc in processes:
# #         proc.start()
# #     for proc in processes:
# #         proc.join()
# #
# #
# #
# # def apply_clean_phone_number_2308(row,name_column):
# #     # row = '07103/839008 / 0909363692 071 03766669'
# #     # remove các kí tự không phải là số, ngoại trừ 2 trường hợp dau / va dau space dau -
# #     pattern_number = re.compile('[^0-9/ -]', re.IGNORECASE) # remove luôn cả chữ ext và dấu đóng mở ngoặc
# #     row = re.sub(pattern_number,"",str(row))
# #     #check truong hợp trước khi split mà được sdt thì ta sẽ ưu tiên remove  nó trước
# #
# #     #thi thoang co doan can split = space, xem xet lai doan code.
# #     #split = dau / hoac dau space ( dau cach), dấu -
# #     # split_character = ['/',' ','-']
# #     # for character in split_character:
# #
# #     list_number  = [numb for numb in re.split('/| |-',row) if len(numb)>0]
# #     dict_phone = defaultdict(list)
# #     for i, number in enumerate(list_number):
# #         # number ='84939800227'
# #         # lam sach sdt, them so 0 vao dau, hoặc thay thế 84 = số 0
# #         if re.search(re.compile('^84'), number) is not None:
# #             number = number.replace('84', '0', 1)
# #             # nếu có nhiều hơn 1 số 0 ở đầu, thì sẽ replace nó
# #             number = re.sub(re.compile('^00+'), "0", str(number))
# #             # number = number.replace('84', '0', 1)
# #         # add so 0 vào đầu
# #         if re.search(re.compile('^0'), number) is None:
# #             number = '0' + number  # removw list cu bangf phan tu nayd
# #         # do 1 sdt co the vua split bằng / hoặc space, nên ta sẽ chỉ áp dụng split nếu kết quả sau khi split là 1 số cố định hoặc 1 số điện thoại, nếu không ta sẽ gộp 2 kết quả split lại, thử xem nó có là số điện thoại hay không
# #         if classify_fixedphone_mobiphone(number) != 'other':
# #             dict_phone[classify_fixedphone_mobiphone(number)].append(number)
# #         elif i>0 and classify_fixedphone_mobiphone(list_number[i-1] + list_number[i]) != 'other': # check điều kiện i>0 để loại trường hợp phần tử 0 kết hợp với phần tử -1
# #             dict_phone[classify_fixedphone_mobiphone(list_number[i-1] + list_number[i])].append(list_number[i-1] + list_number[i])
# #         elif i>1 and classify_fixedphone_mobiphone(list_number[i-2]+list_number[i-1] + list_number[i]) != 'other': # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
# #             dict_phone[classify_fixedphone_mobiphone(list_number[i-2]+list_number[i-1] + list_number[i])].append(list_number[i-2]+list_number[i-1] + list_number[i])
# #         elif i > 2 and classify_fixedphone_mobiphone(list_number[i - 3]+list_number[i - 2]+list_number[i - 1] + list_number[
# #             i]) != 'other':  # check điều kiện i>1 để loại trường hợp phần tử 0 kết hợp với phần tử -1
# #             dict_phone[classify_fixedphone_mobiphone(list_number[i - 3]+list_number[i - 2] + list_number[i - 1] + list_number[i])].append(
# #                 list_number[i - 3]+ list_number[i - 2] + list_number[i - 1] + list_number[i])
# #         else:
# #             pass
# #     for key, value in dict_phone.items():
# #         dict_phone[key] = ','.join(value) if len(value) > 0 else None
# #     return dict_phone[name_column] if len(dict_phone[name_column]) >0 else None
# #
# #
# #
# # def update_field_with_regex(collection,regex,new_key):
# #     '''
# #     rename field with regex
# #     :param collection: collection
# #     :param regex:  regex cua key cần được replace bằng new_key. VD: re.compile('nam')
# #     :param new_key:
# #     :return:
# #     '''
# #     #update field with regex
# #     # collection = db['the1']
# #     # new_key = '1_1_thong_tin_nhan_dang_nam_lan5'
# #     bulk = collection.initialize_ordered_bulk_op()
# #     counter = 0
# #     cusor = collection.find()
# #     for doc in cusor:
# #         # doc = list(cusor)[0]
# #         for k in doc:
# #             # k = '1_1_thong_tin_nhan_dang_nam_lan2'
# #             if re.search(regex,k) is not None:
# #                 print('match')
# #                 bulk.find({"_id": doc['_id']}).update_one({"$unset": {k:1}, "$set": {new_key: doc[k]}})
# #                 counter +=1
# #         if counter % 1000 ==0: # update sau khi duyet 1000 document
# #             try:
# #                 bulk.execute()
# #                 bulk = collection.initialize_ordered_bulk_op()
# #             except Exception as error:
# #                 print(error)
# #     if counter % 1000 !=0:
# #         try:
# #             bulk.execute()
# #         except Exception as error:
# #             print(error)
# #
# #
# # def nam_2308():
# #     db = create_connect_to_mongo(database='cic', locahost=True)
# #     # query= list(db['the_tin_dung'].find())
# #     coll_the = db['the_tin_dung']
# #     # rename key : 1．1． Thông tin nhận dạng => 1_1_thong_tin_nhan_dang
# #     coll_the.update_many({}, {'$rename': {'1．1． Thông tin nhận dạng': '1_1_thong_tin_nhan_dang'}})
# #     # rename key : 1_1_thong_tin_nhan_dang.Mã số CIC: => 1_1_thong_tin_nhan_dang.Mã CIC:
# #     coll_the.update_many({}, {'$rename': {'1_1_thong_tin_nhan_dang.Mã số CIC:': '1_1_thong_tin_nhan_dang.Mã CIC:'}})
# #
# #     # remove ban ghi của doanh nghiep
# #     coll_the.delete_many(
# #         {"1_1_thong_tin_nhan_dang.Tên doanh nghiệp:":
# #              {"$exists": 1}},
# #     )
# #
# #     a = list(db['the_tin_dung'].find({}, {'1_1_thong_tin_nhan_dang.no_number': 1, '_id': 0}))
# #     query = list(db['the_tin_dung'].find({}, {'1_1_thong_tin_nhan_dang': 1, '_id': 0}))
# #     df = pd.DataFrame(a[1:2])
# #     df = pd.DataFrame(a[1:2])
# #     data = [list(a.values()) for a in query]
# #     data1 = [a[0] for a in data if len(a) > 0]
# #     # df = pd.DataFrame(data1)
# #     # nam_to_excel(df,r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_output\credit\output\sdt')
# #
# #     # get những subkey của primary key : "1_1_thong_tin_nhan_dang"
# #     name_parent_key = '1_1_thong_tin_nhan_dang'
# #     distinctThingFields = check_unique_key(coll_the, '1_1_thong_tin_nhan_dang')
# #     a = [a['_id'] for a in distinctThingFields['results']]
# #
# #
# # def check_unique_key(coll):
# #     '''
# #     :param coll:  collection trong mongodb
# #     :return all unique key in collection:
# #     '''
# #     mapper = Code("""
# #         function() {
# #                       for (var key in this) { emit(key, null); }
# #                    }
# #     """)
# #     reducer = Code("""
# #         function(key, stuff) { return null; }
# #     """)
# #
# #     distinctThingFields = coll.map_reduce(mapper, reducer
# #         , out = {'inline' : 1}
# #         , full_response = True)
# #     ## do something with distinctThingFields['results']
# #
# #
# #
# #
# #
# #
# # def nam_pos():
# #     db_pos = create_connect_to_mongo(database='PosData',)
# #     # query= list(db['the_tin_dung'].find())
# #     coll_oder = db_pos['Order']
# #     df_order  = pd.DataFrame(list(coll_oder.find({},{'_id':0})))
# #
# #     a = coll_oder.aggregate([
# #         {'$group':
# #             {'_id':'$order_id',
# #             'order_full': {'$addToSet': '$product_id'}}
# #         }
# #         ])
# #     b = coll_oder.aggregate([
# #         {'$group':
# #             {'_id':'$order_id',
# #             'order_full': {'$addToSet': '$name'}}
# #         }
# #         ])
# #
# #     df_order1 = pd.DataFrame(list(a))
# #     df_order2 = pd.DataFrame(list(b))
# #
# #     coll_associate = db_pos['AssociationRule']
# #     df_rule  = pd.DataFrame(list(coll_associate.find({},{'_id':0})))
# #
# #     coll_support = db_pos['SupportRule']
# #     df_support  = pd.DataFrame(list(coll_support.find({},{'_id':0})))
# #
# #     coll_product = db_pos['Product']
# #     df_product  = pd.DataFrame(list(coll_product.find({},{'_id':0})))
# #
# #     support_min = 0.03
# #     lift = 1.5
# #
# #
# #
# #     db_cic = create_connect_to_mongo(locahost=True,database='cic')
# #     collection_name = 'the_tin_dung_original'
# #     coll_the = db_cic[collection_name]
# #     doc = coll_the.find_one()
# #     doc1 = clean_change_key(doc)
# #     doc1 = clean_change_key(doc)
# #
# #
# #
# #     # import_html_to_mongodb_without_lock(1, 1, process_id='1', collection_name='vay')
# #     # run_vay2208()
# #     # import_html_to_mongodb_without_lock(1,1)
# #     # run_vay2208()
# #
# #     # collection_name = 'the_tin_dung'
# #     # import_html_to_mongodb()
# #     # import_html_to_mongodb_without_lock(1,1)
# #     # import_html_to_mongodb_without_lock(1,1)
# #
# #
# # def get_index_tag(soup, full_filename):
# #     """
# #     idea : lấy ra những index của 1 file bằng các tìm những class là bolder trong khai bao internal CSS.  Sau đó kiểm tra việc lấy những index này có thiếu index nào không.
# #     :param soup:
# #     :return:
# #     """
# #     # tao logger
# #
# #     # lay index
# #     style_scc = soup.style
# #     list_css = (style_scc.get_text()).split('.')
# #     list_bold_tag = [x[:x.index('{')].strip() for x in list_css if 'bold' in x]
# #     # tạo regex những class là bold
# #     regex_bold_class = re.compile('|'.join(list_bold_tag))
# #     # tìm structure của file
# #     #
# #
# #     # case ngoai lệ
# #     # regex kiểu 1.1, 1.2, II, I, V,...
# #     index_regex = re.compile('[1-9]\.[1-9]*|(IX\.|IV\.|V?I{0,3}\.)')
# #     level_regex = re.compile('[1-9]\.[1-9]*')
# #
# #     index_tags = soup.find_all(attrs={'class': regex_bold_class})
# #     # index_tags =  soup.find_all(text=index_regex ,attrs={'class': regex_bold_class})
# #
# #     # check xem co thieu index nao khong
# #     index_tags_text = [a.get_text().strip() for a in index_tags]
# #     # đoạn này ta đang bỏ số la mã và chứ số A,B
# #     index_list_raw = [re.search(level_regex, a).group() for a in index_tags_text if
# #                       re.search(level_regex, a) is not None]
# #     index_list = [Index_doc(a) for a in index_list_raw]
# #     index_list.sort()
# #     a = 'check index cho list sau: {}'.format(index_list)
# #     b = "voi raw index cua file la: {}".format(index_tags_text)
# #     logger.debug(b)
# #     logger.debug(a)
# #     index_list.sort()
# #     for i in range(len(index_list)):
# #         if i >= 1:
# #             check = (index_list[i] - index_list[i - 1])
# #             if check != 1:
# #                 a = 'thieu index giua 2 index sau: {} va {} cua file name: {}'.format(index_list[i], index_list[i - 1],
# #                                                                                       full_filename)
# #                 logger.debug(a)
# #                 print(a)
# #             else:
# #                 logger.debug('chuan')
# #     return index_tags
# #
# #
# #
# # def clean_the_tin_dung_part1():
# #     #clean database the tin dung
# #     db = create_connect_to_mongo(database='cic',locahost=True)
# #     # query= list(db['the_tin_dung'].find())
# #     coll_the = db['the_tin_dung']
# #
# #     #remove những bản ghi bị lỗi của báo cáo vay lạc vào
# #     coll_the.delete_many(
# #         {"_id": { "$regex": re.compile('pickle.6')}}
# #     )
# #
# #     # remove ban ghi của doanh nghiep
# #
# #     #rename key : 1．1． Thông tin nhận dạng => 1_1_thong_tin_nhan_dang
# #     coll_the.update_many({},{'$rename':{'1．1． Thông tin nhận dạng':'1_1_thong_tin_nhan_dang'}})
# #
# #     coll_the.delete_many(
# #        {"1_1_thong_tin_nhan_dang.Tên doanh nghiệp:":
# #            {"$exists":1}},
# #     )
# #     # column thong tin dinh dang luon phai co, ta se xoa het nhung document k chua thong tin nhan dang
# #     coll_the.delete_many(
# #         {'1_1_thong_tin_nhan_dang': {'$exists': 0}}
# #     )
# #
# #     # get những subkey của primary key : "1_1_thong_tin_nhan_dang"
# #     # distinctThingFields= check_unique_key(coll_the,'1_1_thong_tin_nhan_dang')
# #
# #     coll_the.update_many({},{'$rename':{'1_1_thong_tin_nhan_dang.Tên khách hàng:':'1_1_thong_tin_nhan_dang.name'}})
# #
# #     # đối những subkey trong primary key 1_1_thong_tin_nhan_dang có tên gần giống nhau thành 1 tên thống nhất
# #     #rename key : 1_1_thong_tin_nhan_dang.Mã số CIC: => 1_1_thong_tin_nhan_dang.Mã CIC:
# #
# #
# #     coll_the.update_many({},{'$rename':{'1_1_thong_tin_nhan_dang.Mã số CIC:':'1_1_thong_tin_nhan_dang.Mã CIC:'}})
# #     coll_the.update_many({},{'$rename':{'1_1_thong_tin_nhan_dang.Mã CIC:':'1_1_thong_tin_nhan_dang.cic_id'}})
# #     update_key_with_regex(collection = coll_the , regex_sub_key = re.compile('iện thoại'), new_key = 'phone_number', primary_key='1_1_thong_tin_nhan_dang')
# #     update_key_with_regex(coll_the,re.compile('ịa chỉ'),'address','1_1_thong_tin_nhan_dang')
# #     update_key_with_regex(coll_the,re.compile('CMND|chứng minh nhân dân'),'CMND','1_1_thong_tin_nhan_dang')
# #
# #     #rename key :
# #     coll_the.update_many({},{'$rename':{'1．2． Thông tin về tổ chức phát hành thẻ ':'1_2_Thong_tin_to_chuc_phat_hanh_the'}})
# #     coll_the.update_many({},{'$rename':{'1．2． Thông tin về tổ chức phát hành thẻ':'1_2_Thong_tin_to_chuc_phat_hanh_the'}})
# #     # field_12= check_unique_key(coll_the,'1_2_Thong_tin_to_chuc_phat_hanh_the') # ok, không có gì phải làm
# #
# #     coll_the.update_many({},{'$rename':{'1．3． Thông tin tài sản đảm bảo':'1_3_Thong_tin_tsdb'}})
# #     # field_12= check_unique_key(coll_the,'1_3_Thong_tin_tsdb')
# #     # có trường hợp 1 trường lúc là list lúc là dict thi sao?????????
# #
# #     #rename key :
# #     coll_the.update_many({},{'$rename':{'2．1 Diễn biến dư nợ 12 tháng gần nhất ':'2_1_dien_bien_du_no_12_thang'}})
# #     coll_the.update_many({},{'$rename':{'2．1 Diễn biến dư nợ 12 tháng gần nhất':'2_1_dien_bien_du_no_12_thang'}})
# #     # field_12= check_unique_key(coll_the,'2_1_dien_bien_du_no_12_thang') # ok, không có gì phải làm
# #
# #
# #     # 2．1 Diễn biến dư nợ 12 tháng gần nhất
# #     coll_the.update_many({},{'$rename':{'2．1 Diễn biến dư nợ 12 tháng gần nhất ':'2_1_dien_bien_du_no_12_thang'}})
# #     coll_the.update_many({},{'$rename':{'2．1 Diễn biến dư nợ 12 tháng gần nhất':'2_1_dien_bien_du_no_12_thang'}})
# #     # field_12= check_unique_key(coll_the,'2_1_dien_bien_du_no_12_thang') # ok, không có gì phải làm
# #
# #
# #     # 2．1． Thông tin về số tiền thanh toán thẻ của chủ thẻ
# #     coll_the.update_many({},{'$rename':{'2．1． Thông tin về số tiền thanh toán thẻ của chủ thẻ ':'2_1_thong_tin_so_tien_thanh_toan_chu_the'}})
# #     coll_the.update_many({},{'$rename':{'2．1． Thông tin về số tiền phải thanh toán thẻ của chủ thẻ ':'2_1_thong_tin_so_tien_thanh_toan_chu_the'}})
# #     # field_12= check_unique_key(coll_the,'2_1_thong_tin_so_tien_thanh_toan_chu_the') # ok, không có gì phải làm
# #
# #
# #     # 2．1． Thông tin về số tiền thanh toán thẻ của chủ thẻ
# #     coll_the.update_many({},{'$rename':{'2．1 Tổng hợp dư nợ hiện tại':'2_1_tong_hop_du_no_hien_tai'}})
# #
# #
# #
# #     # 2．2
# #     coll_the.update_many({},{'$rename':{'2．2 Danh sách Tổ chức tín dụng đang quan hệ ':'2_2_danh_sach_TCTD_da_quan_he'}})
# #     coll_the.update_many({},{'$rename':{'2．2 Danh sách Tổ chức tín dụng đã từng quan hệ ':'2_2_danh_sach_TCTD_da_quan_he'}})
# #     coll_the.update_many({},{'$rename':{'2．2． Danh sách Tổ chức tín dụng đang quan hệ ':'2_2_danh_sach_TCTD_da_quan_he'}})
# #     # field_12= check_unique_key(coll_the,'2_2_danh_sach_TCTD_da_quan_he') # ok, không có gì phải làm
# #
# #
# #     coll_the.update_many({},{'$rename':{'2．2．1． Từ 15/8/2013 về trước ':'2_2_lich_su_cham_thanh_toan_truoc_2013'}})
# #     coll_the.update_many({},{'$rename':{'2．2．2． Từ 15/8/2013 đến nay ':'2_2_lich_su_cham_thanh_toan_sau_2013'}})
# #
# #     correct_key_3_tong_du_no_tin_dung(coll_the)
# #
# #     # Xoa field .đa số các giá trị của 2.2 Lịch sử chậm thanh toán thẻ của chủ thẻ đều có 2 bảng sub ở trong, nên ta sẽ k cần key này nữa
# #     coll_the.update_many({'2．2． Lịch sử chậm thanh toán thẻ của chủ thẻ ':{'$exists':1}},{"$unset": {'2．2． Lịch sử chậm thanh toán thẻ của chủ thẻ ': 1}})
# #     coll_the.update_many({'2．3 Tình hình thanh toán thẻ của chủ thẻ ':{'$exists':1}},{"$unset": {'2．3 Tình hình thanh toán thẻ của chủ thẻ ': 1}})
# #     coll_the.update_many({'2．3． Tình hình thanh toán thẻ của chủ thẻ ':{'$exists':1}},{"$unset": {'2．3． Tình hình thanh toán thẻ của chủ thẻ ': 1}})
# #     coll_the.update_many({' 2．3． Dư nợ đã bán cho Công ty quản lý tài sản của các TCTD Việt Nam (VAMC)  ':{'$exists':1}},{"$unset": {' 2．3． Dư nợ đã bán cho Công ty quản lý tài sản của các TCTD Việt Nam (VAMC)  ': 1}})
# #     coll_the.update_many({'2．4． Diễn biến dư nợ 12 tháng gần nhất':{'$exists':1}},{"$unset": {'2．4． Diễn biến dư nợ 12 tháng gần nhất': 1}})
# #     coll_the.update_many({' 2．5．	Lịch sử nợ xấu tín dụng trong 03 năm gần nhất ':{'$exists':1}},{"$unset": {' 2．5．	Lịch sử nợ xấu tín dụng trong 03 năm gần nhất ': 1}})
# #     coll_the.update_many({' 2．6． Nợ cần chú ý trong vòng 12 tháng gần nhất ':{'$exists':1}},{"$unset": {' 2．6． Nợ cần chú ý trong vòng 12 tháng gần nhất ': 1}})
# #     coll_the.update_many({'2． TÌNH HÌNH THANH TOÁN CỦA CHỦ THẺ ':{'$exists':1}},{"$unset": {'2． TÌNH HÌNH THANH TOÁN CỦA CHỦ THẺ ': 1}})
# #
# #
# # import numpy as np
# # from sklearn.model_selection import StratifiedShuffleSplit
# # X = np.array([[1, np.NaN], [3.0, 4], [1, 2], [3, 4]])
# # y = np.array([0, 1, 1])
# # sss = StratifiedShuffleSplit(n_splits=3, test_size=0.5, random_state=0)
# # sss.get_n_splits(X, y)
# #
# # print(sss)
# #
# # for train_index, test_index in sss.split(X, y):
# #    print("TRAIN:", train_index, "TEST:", test_index)
# #    X_train, X_test = X[train_index], X[test_index]
# #    y_train, y_test = y[train_index], y[test_index]
# #
# #
# # gmaps = googlemaps.Client(key='AIzaSyBx5w5O4XIfBYEMLxbdYif7TWJgtfQgcqc')
# #
# # # Geocoding an address
# # geocode_result = gmaps.geocode('16 TRANG TRINH , SON TAY, TP. HA NOI')
# # geocode_result = gmaps.geocode('16 TRANG TRINH , SON TAY, HA NOI')
# # geocode_result1 = gmaps.geocode('1600 Amphitheatre Parkway, Mountain View, CA')
#
#
#
# import googlemaps
# from datetime import datetime
# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from bs4 import BeautifulSoup
# import  re
# import pandas as pd
# from nam_basic import  create_connect_to_mongo
# import time
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait
#
#
#
#
#
# db = create_connect_to_mongo(database='cic', locahost=True)
# # query= list(db['the_tin_dung'].find())
# coll_cus = db['customer']
# query = coll_cus.find({},{'address':1})
# driver = webdriver.Firefox()
#
# for doc in query:
#     input_vaues = doc['address']
#     list_address = []
#     for input_vaue in input_vaues:
#         try:
#             address = None
#             driver.get('http://vi.mygeoposition.com/?lang=vi')
#             time.sleep(2)
#             input = driver.find_element_by_id('query')
#             # input_vaue = 'P15/A11 PHUONG MAI_Q.DONG DA_TP.HA NOI'
#             input.send_keys(input_vaue, Keys.RETURN)
#             time.sleep(2)
#             # chọn form geodata
#             try:                # nếu có alert thì click chọn
#                 driver.switch_to.alert.accept()
#             except Exception as e:
#                 # print(e)
#                 pass
#             geo_form = driver.find_element_by_xpath("//ul/li[3]/a")
#             geo_form.click()
#             source = driver.page_source
#
#
#             soup = BeautifulSoup(source,'html.parser')
#             # a = soup.find(text= re.compile('Địa chỉ'))
#             # if 'Tìm thấy nhiều vị trí - hãy chọn một' in soup._find_one(text='Tìm thấy nhiều vị trí - hãy chọn một') is not None : # có nhiều kết quả được chọn khi call
#             try:
#                 # a = soup.find_one(text='Tìm thấy nhiều vị trí - hãy chọn một')
#                 # if a is not # có nhiều kết quả được chọn khi call
#                 # 'html.js.backgroundsize.no-flash.finetuneMode body.page-home div.container.containsAd div.messages div.message.error p button.close'
#                 select_form = driver.find_element_by_xpath("//p/button")
#                 select_form.click()
#             except:
#                 pass
#             df = pd.read_html(source)[0]
#             address = df[df[0]=='Địa chỉ:'][1].values[0]
#             address = df[df[0]=='Vĩ độ:'][1].values[0]
#             address = df[df[0]=='Kinh độ:'][1].values[0]
#             address = df[df[0]=='Khu vực quản lý:'][1].values[0]
#             address = df[df[0]=='Quận:'][1].values[0]
#             address = df[df[0]=='Khu vực quản lý cấp dưới:'][1].values[0]
#             address = df[df[0]=='Vị trí:'][1].values[0]
#             address = df[df[0]=='Đường chính:'][1].values[0]
#             address = df[df[0]=='Đường chính #:'][1].values[0]
#             if address =='Hà Nội, Hoàn Kiếm, Hà Nội, Việt Nam':
#                 address = None
#             # all = df.T.to_json()
#             # dict_new = {'address':address,'all':all}
#             dict_new = {'address':address}
#             list_address.append(dict_new)
#         except Exception as e:
#             driver.close()
#             driver = webdriver.Firefox()
#             print(e)
#     coll_cus.update_one(
#          {'_id': doc['_id']},
#         {'$set':{'address_new': list_address}})
#
#
#
#
import googlemaps
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
from nam_basic import create_connect_to_mongo
import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

db = create_connect_to_mongo(database='cic', locahost=True)
# query= list(db['the_tin_dung'].find())
coll_cus = db['customer']
coll_cus.update_many({},{'$unset': {'address_new':1}})
query = coll_cus.find({'address_new': {'$exists': 0}}, {'address': 1}, no_cursor_timeout=True)
driver = webdriver.Firefox()


def call_api_address(query):
    for doc in query:
        try:
            input_vaues = doc['address']
            list_address = []
            for input_vaue in input_vaues:
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
                    driver.get('http://vi.mygeoposition.com/?lang=vi')
                    # time.sleep(1)
                    input = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "query")))

                    # input_vaue = 'P15/A11 PHUONG MAI_Q.DONG DA_TP.HA NOI'
                    input.send_keys(input_vaue, Keys.RETURN)
                    # input_vaue = '22/54/9 TO42-THUONG DINH-T/XUAN'
                    # time.sleep(1)
                    # chọn form geodata
                    try:  # nếu có alert thì click chọn
                        WebDriverWait(driver, 4).until(EC.alert_is_present())
                        driver.switch_to.alert.accept()
                    except Exception as e:
                        # print(e)
                        pass
                    geo_form = driver.find_element_by_xpath("//ul/li[3]/a")
                    geo_form.click()
                    source = driver.page_source

                    soup = BeautifulSoup(source, 'html.parser')
                    # a = soup.find(text= re.compile('Địa chỉ'))
                    # if 'Tìm thấy nhiều vị trí - hãy chọn một' in soup._find_one(text='Tìm thấy nhiều vị trí - hãy chọn một') is not None : # có nhiều kết quả được chọn khi call
                    try:
                        # a = soup.find_one(text='Tìm thấy nhiều vị trí - hãy chọn một')
                        # check lai nhung truong  hợp có nhiều kết quả thì lấy hết
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
                except Exception as e:
                    driver.close()
                    driver = webdriver.Firefox()
                    print(e)
                    print('loi tai adress: {}'.format(input_vaues))
            coll_cus.update_one(
                {'_id': doc['_id']},
                {'$set': {'address_new': list_address}})
        except:
            driver.close()
            driver = webdriver.Firefox()

    query.close()

    # db = create_connect_to_mongo(database='cic', locahost=True)
    # # query= list(db['the_tin_dung'].find())
    # coll_cus = db['customer']
    # # coll_cus.update_many({},{'$unset': {'address_new':1}})
    # # query những document mà không có trường address_new (chú ý nếu cần thì có thể query những thằng có address_new.address = null)
    # query = list(coll_cus.find({'address_new':{'$exists':0}},{'address':1}))
    # coll_cus_name = 'customer'
    # lock = Lock()
    # # call_api_address(query,lock,coll_cus_name)
    # call_api_address_withou_try(query,lock,coll_cus_name)
    # run_call_api()

    db = create_connect_to_mongo(database='cic', locahost=True)
    # query= list(db['the_tin_dung'].find())
    coll_cus = db['customer']
    # coll_cus.update_many({},{'$unset': {'address_new':1}})
    # query những document mà không có trường address_new (chú ý nếu cần thì có thể query những thằng có address_new.address = null)
    query = list(coll_cus.find({'address_new':{'$exists':0}},{'address':1}))
    coll_cus_name = 'customer'
    lock = Lock()
    #chia list_vay thành 6 phần. bằng function được định nghĩa trong nam_basic.
    processes = [Process(target=call_api_address, args=(list_file,lock,coll_cus_name)) for i,list_file in enumerate(split_list_to_N_equal_element(query,10))]
    for proc in processes:
        proc.start()
    for proc in processes:
        proc.join()

import pandas as pd
from nam_basic import nam_to_excel
df = pd.read_html('http://vnr500.com.vn/Charts/Index?chartId=2')[0]
df.head()


df['new'] = df['Doanh nghiệp'].apply(lambda x: x.split('CEO:')[0])
nam_to_excel(df,r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_output\scrape website\doanh_nghiep')
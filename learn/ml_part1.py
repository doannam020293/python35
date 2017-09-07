import pandas as pd
from sklearn.preprocessing import Imputer, StandardScaler, LabelEncoder, LabelBinarizer, OneHotEncoder
from sklearn.base import TransformerMixin,BaseEstimator
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.model_selection import StratifiedShuffleSplit
# from sklearn.cross_validation import StratifiedShuffleSplit
import numpy as np
from matplotlib import pyplot as plt


df_full  = pd.read_csv(r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_input\learn\housing.csv')
# df.info()
df = df_full.drop('median_house_value',axis=1).values
df_label = df_full['median_house_value'].copy().values

split = StratifiedShuffleSplit(train_size=0.8,random_state=1,n_splits=1)
train_index , test_index  = list(split.split(df,df_label))
train_index , test_index  = list(split.split(df.values)[0]

for train_index , test_index in split.split(df,df_label):
    print(train_index)

list_num_column = list(df.select_dtypes(include=['float64']).columns)
df_num = df.select_dtypes(include=['float64'])
transform = Imputer()
transform.fit(df_num)
a = transform.transform(df_num)
df_num_fill = pd.DataFrame(a,columns=list_num_column)
scaler = StandardScaler()
scaler.fit(df_num_fill)
df_num_scale = scaler.transform(df_num_fill)
df_num_scale = pd.DataFrame(df_num_scale,columns=list_num_column)
idx_total_bedrooms,idx_population  = (4,5)




class CustomTransformer(TransformerMixin, BaseEstimator):
    def __init__(self, create_per_room =True):
        self.create_per_room = create_per_room
    def fit(self, X, y = None):
        return self
    def transform(self,X, y = None):
        # bedroom_per_person = X.iloc[:,idx_total_bedrooms]/X.iloc[:,idx_population ]
        X["rooms_per_household"] = X["total_rooms"] / X["households"]
        X["bedrooms_per_room"] = X["total_bedrooms"] / X["total_rooms"]
        X["population_per_household"] = X["population"] / X["households"]
        # X['bedroom_per_person'] = bedroom_per_person
        return X





housing = pd.read_csv(r'C:\Users\Windows 10 TIMT\OneDrive\Nam\OneDrive - Five9 Vietnam Corporation\work\data_input\learn\housing.csv')
housing['income_cat'] = np.ceil(housing['median_income']/1.5)
housing["income_cat"].where(housing["income_cat"] < 5, 5.0, inplace=True)
housing.hist(bins=50)
split = StratifiedShuffleSplit(train_size=0.8,random_state=1,n_splits=1)
train_index , test_index  = list(split.split(housing,housing["income_cat"]))[0]
# train_index , test_index  = list(split.split(housing,housing["ocean_proximity"]))[0]
# train_index , test_index  = list(split.split(housing,housing["median_income"]))[0]
strat_train_set = housing.loc[train_index]
strat_test_set = housing.loc[test_index]

strat_test_set.drop(labels = 'income_cat',axis = 1,inplace  = True)
strat_train_set.drop(labels = 'income_cat',axis = 1,inplace  = True)


housing = strat_train_set.copy()
housing_label = housing['median_house_value'].copy()
housing = housing.drop(labels = 'median_house_value', axis = 1 , inplace =True)
housing = housing.drop("median_house_value", axis=1)

list_num_column = list(df.select_dtypes(include=['float64']).columns)
df_num = df.select_dtypes(include=['float64'])
transform_fillna = Imputer()
# transform.fit(df_num)
# a = transform.transform(df_num)
# df_num_fill = pd.DataFrame(a,columns=list_num_column)
scaler = StandardScaler()
# scaler.fit(df_num_fill)
# df_num_scale = scaler.transform(df_num_fill)
# df_num_scale = pd.DataFrame(df_num_scale,columns=list_num_column)

custom_transform = CustomTransformer()
# a = custom_transform.transform(df)


housing_cat = housing['ocean_proximity'].copy()
label_encode = LabelEncoder()
label_encode.fit(housing_cat)
label_encode.fit_transform(housing_cat)


pipeline = Pipeline(
    ('fillna',transform_fillna),
    ('create new',custom_transform ),
    ('scale', scaler)
)

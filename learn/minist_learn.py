from sklearn.datasets import fetch_mldata
from sklearn import datasets
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, precision_recall_fscore_support
import cv2 as cv

# trong folder data cua this project  có chứa file mnist-original để load file
dataset = fetch_mldata('mnist-original', data_home='data')
X, y  = dataset['data'], dataset['target']
y_5 = (y==5)
y_full_test = y[56000:]


X_train,y_train  = X[:56000],y_5[:56000]
X_test,y_test = X[56000:],y_5[56000:]

#
# standard_scaler = StandardScaler()
# X_train = standard_scaler.fit_transform(X_train)

clf = SGDClassifier()
clf.fit(X_train, y_train)
# digits = datasets.load_digits()


clf.predict(np.array(X_test[0]).reshape(1,-1))
y_pred = clf.predict(X_test)
confuse_matrix = confusion_matrix(y_test,y_pred)
precision_recall_fscore_support = precision_recall_fscore_support()
precision_recall_fscore_support(y_test, y_pred)
# lay ra index cua false negative
a = (y_test==True) & (y_pred==False)
y_full_test[a ==True]
y_test[a ==True]
#show file ảnh bằng open cv
cv.imshow('nam',X_test[a ==True][0].reshape(28,28))

confuse_matrix/confuse_matrix.sum(axis = 1 )

def shift_image(X_train, shift):
    for i in len(X_train):
        i  = 0
        x = X_train[i].reshape(28,28)
        added = np.zeros(28)
        added = np.zeros(28)
        if shift == 'left':
            x_new = np.c_[added, x[:,1:]]
        elif shift == 'right':
            x_new = np.c_[added, x[:,:-1]]
        elif shift == 'above':
            x_new = np.c_[added, x[1:,:]]
        elif shift == 'right':
            x_new = np.c_[added, x[:,1:]]



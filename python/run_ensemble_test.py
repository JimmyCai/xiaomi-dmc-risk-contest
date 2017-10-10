import pandas as pd
import os
from sklearn.linear_model import LogisticRegression
import sys
from sklearn.preprocessing import OneHotEncoder
from sklearn import metrics
import numpy as np

if __name__ == "__main__":
    train_xgb = pd.read_csv('./ensemble/test/train/cailiming_xgb.csv')
    train_fm = pd.read_csv('./ensemble/test/train/cailiming_fm.csv')
    train_gbdt = pd.read_csv('./ensemble/test/train/cailiming_gbdt_smooth_value.csv')
    train_dart = pd.read_csv('./ensemble/test/train/cailiming_dart_smooth_value.csv')
    train_rf = pd.read_csv('./ensemble/test/train/cailiming_rf_smooth_value.csv')
    train_goss = pd.read_csv('./ensemble/test/train/cailiming_goss_smooth_value.csv')
    train_label = pd.read_csv('./ensemble/test/train/cailiming_label.csv')

    dfTrain = pd.DataFrame(train_gbdt['user_id'], columns=['user_id'])
    dfTrain['xgb'] = train_xgb['ovd_rate']
    dfTrain['fm'] = train_fm['ovd_rate']
    dfTrain['gbdt'] = train_gbdt['ovd_rate']
    dfTrain['dart'] = train_dart['ovd_rate']
    dfTrain['rf'] = train_rf['ovd_rate']
    dfTrain['goss'] = train_goss['ovd_rate']
    dfTrain['label'] = train_label['label']

    test_xgb = pd.read_csv('./ensemble/test/test/cailiming_xgb.csv')
    test_fm = pd.read_csv('./ensemble/test/test/cailiming_fm.csv')
    test_gbdt = pd.read_csv('./ensemble/test/test/cailiming_gbdt_smooth_value.csv')
    test_dart = pd.read_csv('./ensemble/test/test/cailiming_dart_smooth_value.csv')
    test_rf = pd.read_csv('./ensemble/test/test/cailiming_rf_smooth_value.csv')
    test_goss = pd.read_csv('./ensemble/test/test/cailiming_goss_smooth_value.csv')
    test_label = pd.read_csv('./ensemble/test/test/cailiming_label.csv')

    dfTest = pd.DataFrame(test_gbdt['user_id'], columns=['user_id'])
    dfTest['xgb'] = test_xgb['ovd_rate']
    dfTest['fm'] = test_fm['ovd_rate']
    dfTest['gbdt'] = test_gbdt['ovd_rate']
    dfTest['dart'] = test_dart['ovd_rate']
    dfTest['rf'] = test_rf['ovd_rate']
    dfTest['goss'] = test_goss['ovd_rate']
    dfTest['label'] = test_label['label']

    #enc = OneHotEncoder()
    feats = ['xgb', 'fm', 'gbdt', 'dart', 'rf', 'goss']
    for i,feat in enumerate(feats):
        x_train = dfTrain[feat].values.reshape(-1, 1) #enc.fit_transform(dfTrain[feat].values.reshape(-1, 1))
        x_test = dfTest[feat].values.reshape(-1, 1) #enc.fit_transform(dfTest[feat].values.reshape(-1, 1))
        if i == 0:
            X_train, X_test = x_train, x_test
        else:
            X_train, X_test = np.hstack((X_train, x_train)), np.hstack((X_test, x_test))

    lr = LogisticRegression(C=0.02, tol=1e-6, dual=False, solver='liblinear', max_iter=10000, penalty='l1', verbose=0, n_jobs=1)
    lr.fit(X_train, dfTrain['label'].values)

    proba_train = lr.predict_proba(X_train)[:,1]
    train_auc = metrics.roc_auc_score(dfTrain['label'].values, proba_train)
    print train_auc

    proba_test = lr.predict_proba(X_test)[:,1]
    dfResult = pd.DataFrame(test_gbdt['user_id'], columns=['user_id'])
    dfResult['ovd_rate']=proba_test
    dfResult.to_csv('./ensemble/test/test/cailiming_xgb_fm_gbdt_rf_dart_goss.csv', index=None, encoding='utf-8')

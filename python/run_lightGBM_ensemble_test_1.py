import os
from sklearn.linear_model import LogisticRegression
import sys
from sklearn.preprocessing import OneHotEncoder
from sklearn import metrics
import numpy as np

if __name__ == "__main__":
    train_gbdt_fy = pd.read_csv('./predict/test/train/cailiming_gbdt_full_year.csv')
    train_gbdt_ts = pd.read_csv('./predict/test/train/cailiming_gbdt_three_season.csv')
    train_gbdt_hy = pd.read_csv('./predict/test/train/cailiming_gbdt_half_year.csv')
    train_gbdt_os = pd.read_csv('./predict/test/train/cailiming_gbdt_one_season.csv')

    train_goss_fy = pd.read_csv('./predict/test/train/cailiming_goss_full_year.csv')
    train_goss_ts = pd.read_csv('./predict/test/train/cailiming_goss_three_season.csv')
    train_goss_hy = pd.read_csv('./predict/test/train/cailiming_goss_half_year.csv')
    train_goss_os = pd.read_csv('./predict/test/train/cailiming_goss_one_season.csv')

    train_dart_fy = pd.read_csv('./predict/test/train/cailiming_dart_full_year.csv')
    train_dart_ts = pd.read_csv('./predict/test/train/cailiming_dart_three_season.csv')
    train_dart_hy = pd.read_csv('./predict/test/train/cailiming_dart_half_year.csv')
    train_dart_os = pd.read_csv('./predict/test/train/cailiming_dart_one_season.csv')

    train_label = pd.read_csv('./predict/test/train/cailiming_label.csv')

    dfTrain = pd.DataFrame(train_fy['user_id'], columns=['user_id'])
    dfTrain['gbdt_fy'] = train_gbdt_fy['ovd_rate']
    dfTrain['gbdt_ts'] = train_gbdt_ts['ovd_rate']
    dfTrain['gbdt_hy'] = train_gbdt_hy['ovd_rate']
    dfTrain['gbdt_os'] = train_gbdt_os['ovd_rate']

    dfTrain['goss_fy'] = train_goss_fy['ovd_rate']
    dfTrain['goss_ts'] = train_goss_ts['ovd_rate']
    dfTrain['goss_hy'] = train_goss_hy['ovd_rate']
    dfTrain['goss_os'] = train_goss_os['ovd_rate']

    dfTrain['dart_fy'] = train_dart_fy['ovd_rate']
    dfTrain['dart_ts'] = train_dart_ts['ovd_rate']
    dfTrain['dart_hy'] = train_dart_hy['ovd_rate']
    dfTrain['dart_os'] = train_dart_os['ovd_rate']

    dfTrain['label'] = train_label['label']

    test_gbdt_fy = pd.read_csv('./predict/test/test/cailiming_gbdt_full_year.csv')
    test_gbdt_ts = pd.read_csv('./predict/test/test/cailiming_gbdt_three_season.csv')
    test_gbdt_hy = pd.read_csv('./predict/test/test/cailiming_gbdt_half_year.csv')
    test_gbdt_os = pd.read_csv('./predict/test/test/cailiming_gbdt_one_season.csv')

    test_goss_fy = pd.read_csv('./predict/test/test/cailiming_goss_full_year.csv')
    test_goss_ts = pd.read_csv('./predict/test/test/cailiming_goss_three_season.csv')
    test_goss_hy = pd.read_csv('./predict/test/test/cailiming_goss_half_year.csv')
    test_goss_os = pd.read_csv('./predict/test/test/cailiming_goss_one_season.csv')

    test_dart_fy = pd.read_csv('./predict/test/test/cailiming_dart_full_year.csv')
    test_dart_ts = pd.read_csv('./predict/test/test/cailiming_dart_three_season.csv')
    test_dart_hy = pd.read_csv('./predict/test/test/cailiming_dart_half_year.csv')
    test_dart_os = pd.read_csv('./predict/test/test/cailiming_dart_one_season.csv')

    dfTest = pd.DataFrame(test_fy['user_id'], columns=['user_id'])
    dfTest['gbdt_fy'] = test_gbdt_fy['ovd_rate']
    dfTest['gbdt_ts'] = test_gbdt_ts['ovd_rate']
    dfTest['gbdt_hy'] = test_gbdt_hy['ovd_rate']
    dfTest['gbdt_os'] = test_gbdt_os['ovd_rate']

    dfTest['goss_fy'] = test_goss_fy['ovd_rate']
    dfTest['goss_ts'] = test_goss_ts['ovd_rate']
    dfTest['goss_hy'] = test_goss_hy['ovd_rate']
    dfTest['goss_os'] = test_goss_os['ovd_rate']

    dfTest['dart_fy'] = test_dart_fy['ovd_rate']
    dfTest['dart_ts'] = test_dart_ts['ovd_rate']
    dfTest['dart_hy'] = test_dart_hy['ovd_rate']
    dfTest['dart_os'] = test_dart_os['ovd_rate']

    #enc = OneHotEncoder()
    feats = ['gbdt_fy', 'gbdt_ts', 'gbdt_hy', 'gbdt_os', 'goss_fy', 'goss_ts', 'goss_hy', 'goss_os', 'dart_fy', 'dart_ts', 'dart_hy', 'dart_os']
    for i,feat in enumerate(feats):
        x_train = dfTrain[feat].values.reshape(-1, 1) #enc.fit_transform(dfTrain[feat].values.reshape(-1, 1))
        x_test = dfTest[feat].values.reshape(-1, 1) #enc.fit_transform(dfTest[feat].values.reshape(-1, 1))
        if i == 0:
            X_train, X_test = x_train, x_test
        else:
            X_train, X_test = np.hstack((X_train, x_train)), np.hstack((X_test, x_test))

    lr = LogisticRegression(C=0.1, tol=1e-6, dual=False, solver='liblinear', max_iter=10000, penalty='l1', verbose=0, n_jobs=1)
    lr.fit(X_train, dfTrain['label'].values)

    proba_train = lr.predict_proba(X_train)[:,1]
    train_auc = metrics.roc_auc_score(dfTrain['label'].values, proba_train)
    print train_auc

    proba_test = lr.predict_proba(X_test)[:,1]
    dfAns = pd.DataFrame(test_fy['user_id'], columns=['user_id'])
    dfAns['ovd_rate'] = proba_test[:,1]
    dfAns.to_csv('./predict/test/test/cailiming_gbdt_lr_ensemble.csv', index=None, encoding='utf-8')
    
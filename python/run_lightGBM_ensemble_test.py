import os
from sklearn.linear_model import LogisticRegression
import sys
from sklearn.preprocessing import OneHotEncoder
from sklearn import metrics
import numpy as np

if __name__ == "__main__":
    train_fy = pd.read_csv('./predict/test/train/cailiming_gbdt_full_year.csv')
    train_ts = pd.read_csv('./predict/test/train/cailiming_gbdt_three_season.csv')
    train_hy = pd.read_csv('./predict/test/train/cailiming_gbdt_half_year.csv')
    train_os = pd.read_csv('./predict/test/train/cailiming_gbdt_one_season.csv')
    train_label = pd.read_csv('./predict/test/train/cailiming_label.csv')

    dfTrain = pd.DataFrame(train_fy['user_id'], columns=['user_id'])
    dfTrain['fy'] = train_fy['ovd_rate']
    dfTrain['ts'] = train_ts['ovd_rate']
    dfTrain['hy'] = train_hy['ovd_rate']
    dfTrain['os'] = train_os['ovd_rate']
    dfTrain['label'] = train_label['label']

    test_fy = pd.read_csv('./predict/test/test/cailiming_gbdt_full_year.csv')
    test_ts = pd.read_csv('./predict/test/test/cailiming_gbdt_three_season.csv')
    test_hy = pd.read_csv('./predict/test/test/cailiming_gbdt_half_year.csv')
    test_os = pd.read_csv('./predict/test/test/cailiming_gbdt_one_season.csv')

    dfTest = pd.DataFrame(test_fy['user_id'], columns=['user_id'])
    dfTest['fy'] = test_fy['ovd_rate']
    dfTest['ts'] = test_ts['ovd_rate']
    dfTest['hy'] = test_hy['ovd_rate']
    dfTest['os'] = test_os['ovd_rate']

    #enc = OneHotEncoder()
    feats = ['fy', 'ts', 'hy', 'os']
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
    dfAns.to_csv('./predict/test/test/cailiming_gbdt_ensemble.csv', index=None, encoding='utf-8')
    
import pandas as pd
import numpy as np

xgb = pd.read_csv('./cailiming_adj_par_20171008_87041.csv')
fm = pd.read_csv('./cailiming_fm_20171006_85940.csv')
other_tree = pd.read_csv('./cailiming_gbdt_rf_dart_goss_86320.csv')

xgb['ovd_rate'] = xgb['ovd_rate'].rank()
fm['ovd_rate'] = fm['ovd_rate'].rank()
other_tree['ovd_rate'] = other_tree['ovd_rate'].rank()

Uid = xgb['user_id']
pred = (0.6 * xgb['ovd_rate'] + 0.25 * other_tree['ovd_rate'] + 0.15 * fm['ovd_rate'])/10000

ans = pd.DataFrame(Uid, columns=['user_id'])
ans['ovd_rate'] = pred
ans.to_csv('./cailiming_xgb_fm_othertree_rank_20171010.csv', index=None, encoding='utf-8')

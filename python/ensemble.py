import pandas as pd
import numpy as np

xgb = pd.read_csv('./cailiming_adj_par_20171008_87041.csv')
fm = pd.read_csv('./cailiming_fm_20171006_85940.csv')

Uid = xgb['user_id']
pred = 0.65 * xgb['ovd_rate'] + 0.35 * fm['ovd_rate']

ans = pd.DataFrame(Uid, columns=['user_id'])
ans['ovd_rate'] = pred
ans.to_csv('./cailiming_xgb_fm_20171009.csv', index=None, encoding='utf-8')

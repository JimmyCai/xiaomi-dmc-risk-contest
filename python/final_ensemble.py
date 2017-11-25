import pandas as pd
import numpy as np

xgb = pd.read_csv('./test-final/cailiming_final_xgb_with_hy_rate.csv')
gbdt = pd.read_csv('./test-final/cailiming_final_gbdt_ensemble_basicprofile.csv')

Uid = f1['user_id']
pred = 0.2 * xgb['ovd_rate'] + 0.8 * gbdt['ovd_rate']

ans = pd.DataFrame(Uid, columns=['user_id'])
ans['ovd_rate'] = pred
ans.to_csv('./to-submit/cailiming_xgb_gbdt_ensemble.csv', index=None, encoding='utf-8')

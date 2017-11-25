import pandas as pd
import numpy as np

fy = pd.read_csv('./test-final/cailiming_final_gbdt_full_year_basicprofile.csv')
ts = pd.read_csv('./test-final/cailiming_final_gbdt_three_season_basicprofile.csv')
hy = pd.read_csv('./test-final/cailiming_final_gbdt_half_year_basicprofile.csv')
os = pd.read_csv('./test-final/cailiming_final_gbdt_one_season_basicprofile.csv')

Uid = fy['user_id']
pred = 0.25 * fy['ovd_rate'] + 0.25 * ts['ovd_rate'] + 0.25 * hy['ovd_rate'] + 0.25 * os['ovd_rate']

ans = pd.DataFrame(Uid, columns=['user_id'])
ans['ovd_rate'] = pred
ans.to_csv('./to-submit/cailiming_final_gbdt_ensemble_basicprofile.csv', index=None, encoding='utf-8')

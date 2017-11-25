from sklearn import metrics
import pandas as pd
import sys

if __name__ == "__main__":
	predict_path, label_path = sys.argv[1:3]

	predict_df = pd.read_csv(predict_path)
	label_df = pd.read_csv(label_path)

	auc = metrics.roc_auc_score(label_df['label'].values, predict_df['ovd_rate'].values)
	print auc
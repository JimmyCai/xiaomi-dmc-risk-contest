import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.datasets import load_svmlight_file
from sklearn.metrics import classification_report


def offline(X_org, y_org):
	#X_train, X_test, y_train, y_test = train_test_split(X_org, y_org, test_size=0.1, random_state=0)

	param_grid = {
		'n_estimators': [i for i in range(800,1300,50)], 
		'max_depth': [j for j in range(4,7,1)],
		'learning_rate': [0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.055, 0.06, 0.065, 0.07, 0.075, 0.08, 0.085, 0.09]
	}

	model = GradientBoostingClassifier()
	grid_search = GridSearchCV(model, param_grid, n_jobs=3, scoring='roc_auc')
	grid_search.fit(X_org, y_org)

	best_parameters = grid_search.best_estimator_.get_params()
	for para, val in best_parameters.items():
        print para, val
    return best_parameters['n_estimators'],best_parameters['max_depth'],best_parameters['learning_rate']
	
def loadData(path):
	dtrain = load_svmlight_file(path)
	dtrain[0].toarray(), dtrain[1]

def online(X_train, y_train, X_test, test_id_path, output_path, n_estimators, max_depth, learning_rate):
	model = GradientBoostingClassifier(n_estimators = n_estimators, max_depth = max_depth, learning_rate = learning_rate)
	model.fit(X_train, y_train)
	preds = model.predict_proba(X_test)[:,1]

	with open(test_id_path) as f:
		test_content = f.readlines()
	test_id = [x.strip() for x in test_content]

	test_result = pd.DataFrame(test_id,columns=["user_id"])
	test_result["ovd_rate"] = test_y
	test_result.to_csv(output_path, index=None, encoding='utf-8')

def main:
	X_org, y_org = loadData('../xgb/data/test/train/20171001-86843.svm.txt')
	X_test, y_test = loadData('../xgb/data/test/test/20171001-86843.svm.txt')
	n_estimators, max_depth, learning_rate = offline(X_org, y_org)
	print 'n_estimators: ' + str(n_estimators)
	print 'max_depth: ' + str(max_depth)
	print 'learning_rate: ' + str(learning_rate)

	test_id_path = '../xgb/data/test/test/20171001-ins_id.txt'
	output_path = './predict/gbdt.csv'
	online(X_org, y_org, X_test, y_test, test_id_path, output_path, n_estimators, max_depth, learning_rate)

if __name__ == '__main__':
		main()	
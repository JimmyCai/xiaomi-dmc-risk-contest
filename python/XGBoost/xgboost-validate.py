#!/usr/bin/python
import numpy as np
import scipy.sparse
import pickle
import xgboost as xgb
import sys

if __name__ == "__main__":
    train_path, test_path, train_instance_path, test_instance_path, model_path, train_output_path, test_output_path, feature_path = sys.argv[1:9]
    dtrain = xgb.DMatrix(train_path)
    dtest = xgb.DMatrix(test_path)

    param = {'max_depth':5, 'eta':0.025, 'silent':1, 'objective':'binary:logistic', 'lambda': 9, 'subsample': 0.78, 'colsample_bytree': 0.76, 'min_child_weight': 2.32 }
    param['nthread'] = 10
    param['eval_metric'] = 'auc'

    evallist  = [(dtest,'eval'), (dtrain,'train')]

    num_round = 1000
    bst = xgb.train( param, dtrain, num_round, evallist)#, xgb_model=model_path )

    bst.save_model(model_path)

    #write feature score
    imp_writer = open(feature_path, 'w')
    imp_writer.write('feature,score\n')
    imp_score = bst.get_fscore()
    for k, v in imp_score.iteritems():
        imp_writer.write(str(k).replace('f', '') + ',' + str(v) + '\n')
    imp_writer.close()

    #write test predict
    test_preds = bst.predict(dtest)

    with open(test_instance_path) as f:
        test_content = f.readlines()
    test_ins_id = [x.strip() for x in test_content]

    ans_writer = open(test_output_path, 'w')
    ans_writer.write('user_id,ovd_rate\n')
    for i in range(test_preds.shape[0]):
        cur_line = test_ins_id[i] + "," + str(test_preds[i]) + "\n"
        ans_writer.write(cur_line)

    ans_writer.close()

    #write train predict
    train_preds = bst.predict(dtrain)

    with open(train_instance_path) as f:
        train_content = f.readlines()
    train_ins_id = [x.strip() for x in train_content]

    ans_writer = open(train_output_path, 'w')
    ans_writer.write('user_id,ovd_rate\n')
    for i in range(train_preds.shape[0]):
        cur_line = train_ins_id[i] + ',' + str(train_preds[i]) + '\n'
        ans_writer.write(cur_line)

    ans_writer.close()

#!/usr/bin/env bash

SPARK_HOME="spark home path"

LIGHTGBM_PATH="lightGBM install path"

TRAIN_PATH="data/test/train/train_half_year.svm.txt"
TEST_PATH="data/test/test/train_half_year.svm.txt"
TRAIN_INSTANCE_PATH="data/test/train/ins_id_half_year.txt"
TEST_INSTANCE_PATH="data/test/test/ins_id_half_year.txt"
MODEL_PATH="model/test/gbdt_half_year"
TRAIN_OUTPUT_PATH="predict/test/train/cailiming_gbdt_half_year.csv"
TEST_OUTPUT_PATH="predict/test/test/cailiming_gbdt_half_year.csv"

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset/xgb_feature_half_year/*" | awk -F'\t' '{print $3}' > $TRAIN_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset/xgb_feature_half_year/*" | awk -F'\t' '{print $1}' > $TRAIN_INSTANCE_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/testset/xgb_feature_half_year/*" | awk -F'\t' '{print $3}' > $TEST_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/testset/xgb_feature_half_year/*" | awk -F'\t' '{print $1}'  > $TEST_INSTANCE_PATH

$LIGHTGBM_PATH/lightgbm config=train.conf num_trees=500 data=$TRAIN_PATH valid_data='' output_model=$MODEL_PATH

PRED_TMP1="test_pred_tmp1.txt"
PRED_TMP2="test_pred_tmp2.txt"
$LIGHTGBM_PATH/lightgbm task=predict input_model=$MODEL_PATH data=$TRAIN_PATH output_result=$PRED_TMP1
paste -d',' $TRAIN_INSTANCE_PATH $PRED_TMP1 > $PRED_TMP2
cat head.csv $PRED_TMP2 > $TRAIN_OUTPUT_PATH

$LIGHTGBM_PATH/lightgbm task=predict input_model=$MODEL_PATH data=$TEST_PATH output_result=$PRED_TMP1
paste -d',' $TEST_INSTANCE_PATH $PRED_TMP1 > $PRED_TMP2
cat head.csv $PRED_TMP2 > $TEST_OUTPUT_PATH

rm $PRED_TMP1
rm $PRED_TMP2

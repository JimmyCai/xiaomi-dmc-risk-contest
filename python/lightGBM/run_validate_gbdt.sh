#!/usr/bin/env bash

SPARK_HOME="spark home path"

LIGHTGBM_PATH="lightGBM install path"

TRAIN_PATH="data/validate/train/train_full_year.svm.txt"
TEST_PATH="data/validate/test/train_full_year.svm.txt"
TRAIN_INSTANCE_PATH="data/validate/train/ins_id_full_year.txt"
TEST_INSTANCE_PATH="data/validate/test/ins_id_full_year.txt"
MODEL_PATH="model/validate/gbdt_full_year"
TRAIN_OUTPUT_PATH="predict/validate/train/cailiming_gbdt_full_year.csv"
TEST_OUTPUT_PATH="predict/validate/test/cailiming_gbdt_full_year.csv"

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset_train/lightGBM_feature_full_year/*" | awk -F'\t' '{print $3}' > $TRAIN_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset_train/lightGBM_feature_full_year/*" | awk -F'\t' '{print $1}' > $TRAIN_INSTANCE_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset_validate/lightGBM_feature_full_year/*" | awk -F'\t' '{print $3}' > $TEST_PATH

$SPARK_HOME/bin/hadoop --cluster c3prc-hadoop fs -cat "develop/xxx/dataset_validate/lightGBM_feature_full_year/*" | awk -F'\t' '{print $1}'  > $TEST_INSTANCE_PATH

$LIGHTGBM_PATH/lightgbm config=train.conf num_trees=500 boosting_type=gbdt data=$TRAIN_PATH valid_data=$TEST_PATH output_model=$MODEL_PATH

PRED_TMP1="validate_pred_tmp1.txt"
PRED_TMP2="validate_pred_tmp2.txt"
$LIGHTGBM_PATH/lightgbm task=predict input_model=$MODEL_PATH data=$TRAIN_PATH output_result=$PRED_TMP1
paste -d',' $TRAIN_INSTANCE_PATH $PRED_TMP1 > $PRED_TMP2
cat head.csv $PRED_TMP2 > $TRAIN_OUTPUT_PATH

$LIGHTGBM_PATH/lightgbm task=predict input_model=$MODEL_PATH data=$TEST_PATH output_result=$PRED_TMP1
paste -d',' $TEST_INSTANCE_PATH $PRED_TMP1 > $PRED_TMP2
cat head.csv $PRED_TMP2 > $TEST_OUTPUT_PATH

rm $PRED_TMP1
rm $PRED_TMP2

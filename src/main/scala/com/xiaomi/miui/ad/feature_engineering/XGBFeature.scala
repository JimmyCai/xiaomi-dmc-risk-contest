package com.xiaomi.miui.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.miui.ad.others.UALProcessed
import com.xiaomi.miui.ad.statistics.MinMaxStatistics
import com.xiaomi.miui.ad.tools.{FeatureEncodingTools, MergedMethod}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-10-17.
  */
object XGBFeature extends TreeFeature{
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //rate feature start
        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryDetailFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val queryStatFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/query-stat-rate-fields.txt")

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDurationFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageDayFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appUsageTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatInstallFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val appStatOpenTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-open-time-rate-fields.txt")
        //rate feature end

        val needFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/xgb-fields.txt")

        val hyAvgFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/half-year-avg-fields.txt")
        val hyMaxFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/half-year-max-fields.txt")

        val minMaxStatistics = MinMaxStatistics.getMinMaxStatistics(spark, args("minMax"))
        val minMaxStatisticsBroadCast = spark.sparkContext.broadcast(minMaxStatistics)

        val outUser = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/out_user.txt"))
            .getLines()
            .map { line =>
                line.split("\t").head
            }
            .toSet
        val outUserBroadCast = spark.sparkContext.broadcast(outUser)

        val tDF = spark.read.parquet(args("input"))
            .repartition(100)
            .as[UALProcessed]
            .filter { ual =>
                !outUserBroadCast.value.contains(ual.user)
            }
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryDetailFieldBroadCast.value, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryStatFieldBroadCast.value, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDurationFieldsBroadCast.value, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDayFieldsBroadCast.value, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageTimeFieldsBroadCast.value, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatInstallFieldsBroadCast.value, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatOpenTimeFieldsBroadCast.value, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.max)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, hyAvgFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, hyMaxFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.max)

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex, 0)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex, 6)

                FeatureEncoded(ual.user, startIndex - 1, ual.label + featureBuilder.getFeature())
            }


        val ansDF = tDF.orderBy($"user")
            .map { r =>
                r.user + "\t" + r.featureSize + "\t" + r.features
            }

        ansDF
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}

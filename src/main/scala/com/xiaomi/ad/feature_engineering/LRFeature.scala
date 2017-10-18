package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.{MaxChangeMinMaxStatistics, MinMaxStatistics}
import com.xiaomi.ad.tools.{FeatureEncodingTools, MergedMethod}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-9-28.
  */
object LRFeature extends OneHotFeature {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val needFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/new-lr-fields.txt")

        val combineLogFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/combine-log-need-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .toMap
        val combineLogFieldsBroadCast = spark.sparkContext.broadcast(combineLogFields)

        val combineLogNeedFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/combine-log-need-fields.txt"))
            .getLines()
            .flatMap { line =>
                val split = line.split("\t")
                val ss = split.head.split(",")
                Seq(ss.head.toInt, ss.last.toInt)
            }
            .toSet
        val combineLogNeedFieldsBroadCast = spark.sparkContext.broadcast(combineLogNeedFields)

        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val maxChangeFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/max-change-fields.txt")

        val minMaxStatistics = MinMaxStatistics.getMinMaxStatistics(spark, args("minMax"))
        val minMaxStatisticsBroadCast = spark.sparkContext.broadcast(minMaxStatistics)

        val maxChangeMinMaxStatistics = MaxChangeMinMaxStatistics.getMinMaxStatistics(spark, args("maxChangeMinMax"))
        val maxChangeMinMaxStatisticsBroadCast = spark.sparkContext.broadcast(maxChangeMinMaxStatistics)

        val tDF = spark.read.parquet(args("input"))
            .repartition(100)
            .as[UALProcessed]
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)

                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 7)(MergedMethod.avg)

                startIndex = encodeMaxChangeFeature(featureBuilder, ual, startIndex, maxChangeFieldsBroadCast.value, maxChangeMinMaxStatisticsBroadCast.value, 7)

                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 7)

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

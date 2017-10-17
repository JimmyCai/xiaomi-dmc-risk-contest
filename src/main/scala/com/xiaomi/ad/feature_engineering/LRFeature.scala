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

        val queryDetailRateBroadCast = spark.sparkContext.broadcast(
            (131 to 10130)
                .zipWithIndex
                .toMap
        )

        val queryStatRateBroadCast = spark.sparkContext.broadcast(
            (10131 to 10233)
                .zipWithIndex
                .toMap
        )

        val appUsageDurationRateBroadCast = spark.sparkContext.broadcast(
            (10234 to 40180)
                .zipWithIndex
                .toMap
        )

        val appUsageDayRateBroadCast = spark.sparkContext.broadcast(
            (40181 to 68114)
                .zipWithIndex
                .toMap
        )

        val appUsageTimeRateBroadCast = spark.sparkContext.broadcast(
            (68115 to 96048)
                .zipWithIndex
                .toMap
        )

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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

                startIndex = encodeMaxChangeFeature(featureBuilder, ual, startIndex, maxChangeFieldsBroadCast.value, maxChangeMinMaxStatisticsBroadCast.value, 6)

                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 6)

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

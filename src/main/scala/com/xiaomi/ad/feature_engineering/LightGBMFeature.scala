package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.MinMaxStatistics
import com.xiaomi.ad.tools.{FeatureEncodingTools, MergedMethod}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-9-28.
  */
object LightGBMFeature extends TreeFeature {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)

        val sparkConf = new SparkConf()

        argv("time") match {
            case "fullYear" =>
                executeFullYear(argv, sparkConf)
            case "threeSeason" =>
                executeThreeSeason(argv, sparkConf)
            case "halfYear" =>
                executeHalfYear(argv, sparkConf)
            case "oneSeason" =>
                executeOneSeason(argv, sparkConf)
        }
    }

    def executeFullYear(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //rate feature start
        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryDetailFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 131, 10130)

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val queryStatFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10131, 10233)

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDurationFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10234, 40180)

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageDayFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 40181, 68114)

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appUsageTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 68115, 96048)

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatInstallFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val appStatOpenTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-open-time-rate-fields.txt")
        //rate feature end

        val needFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/xgb-fields.txt")

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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.max)

                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryDetailFieldBroadCast.value, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryStatFieldBroadCast.value, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDurationFieldsBroadCast.value, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDayFieldsBroadCast.value, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageTimeFieldsBroadCast.value, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatInstallFieldsBroadCast.value, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatOpenTimeFieldsBroadCast.value, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex, 0)

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

    def executeThreeSeason(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //rate feature start
        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryDetailFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 131, 10130)

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val queryStatFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10131, 10233)

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDurationFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10234, 40180)

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageDayFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 40181, 68114)

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appUsageTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 68115, 96048)

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatInstallFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val appStatOpenTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-open-time-rate-fields.txt")
        //rate feature end

        val tsAvgFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ts-avg-fields.txt")
        val tsMaxFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ts-max-fields.txt")

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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, tsAvgFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, tsMaxFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.max)

                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryDetailFieldBroadCast.value, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryStatFieldBroadCast.value, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDurationFieldsBroadCast.value, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDayFieldsBroadCast.value, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageTimeFieldsBroadCast.value, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatInstallFieldsBroadCast.value, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatOpenTimeFieldsBroadCast.value, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 3)(MergedMethod.avg)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex, 3)

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

    def executeHalfYear(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //rate feature start
        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryDetailFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 131, 10130)

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val queryStatFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10131, 10233)

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDurationFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10234, 40180)

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageDayFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 40181, 68114)

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appUsageTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 68115, 96048)

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatInstallFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val appStatOpenTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-open-time-rate-fields.txt")
        //rate feature end

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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, hyAvgFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, hyMaxFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.max)

                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryDetailFieldBroadCast.value, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryStatFieldBroadCast.value, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDurationFieldsBroadCast.value, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDayFieldsBroadCast.value, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageTimeFieldsBroadCast.value, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatInstallFieldsBroadCast.value, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatOpenTimeFieldsBroadCast.value, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

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

    def executeOneSeason(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        //rate feature start
        val queryDetailRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-detail-rate-fields.txt")

        val queryDetailFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 131, 10130)

        val queryStatRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/query-stat-rate-fields.txt")

        val queryStatFieldBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10131, 10233)

        val appUsageDurationRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-duration-rate-fields.txt")

        val appUsageDurationFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 10234, 40180)

        val appUsageDayRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-day-rate-fields.txt")

        val appUsageDayFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 40181, 68114)

        val appUsageTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-usage-time-rate-fields.txt")

        val appUsageTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, 68115, 96048)

        val appStatInstallRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatInstallFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-install-rate-fields.txt")

        val appStatOpenTimeRateBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/ratefeature/app-open-time-rate-fields.txt")

        val appStatOpenTimeFieldsBroadCast = FeatureEncodingTools.getBroadCastFields(spark, "/ratefeature/app-open-time-rate-fields.txt")
        //rate feature end

        val osAvgFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/one-season-avg-fields.txt")
        val osMaxFieldsBroadCast = FeatureEncodingTools.getBroadCastFieldMap(spark, "/one-season-max-fields.txt")

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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, osAvgFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeFeatures(featureBuilder, ual, startIndex, osMaxFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.max)

                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryDetailFieldBroadCast.value, queryDetailRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, queryStatFieldBroadCast.value, queryStatRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDurationFieldsBroadCast.value, appUsageDurationRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageDayFieldsBroadCast.value, appUsageDayRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appUsageTimeFieldsBroadCast.value, appUsageTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatInstallFieldsBroadCast.value, appStatInstallRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)
                startIndex = encodeRateFeatures(featureBuilder, ual, startIndex, appStatOpenTimeFieldsBroadCast.value, appStatOpenTimeRateBroadCast.value, minMaxStatisticsBroadCast.value, 9)(MergedMethod.avg)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex, 9)

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

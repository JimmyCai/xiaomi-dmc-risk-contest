package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.{MinMax, MinMaxStatistics}
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 17-9-28.
  */
object LRFeature {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val needFields = Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/new-lr-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val needFieldsBroadCast = spark.sparkContext.broadcast(needFields)

        val combineLogFields = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/combine-log-need-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .toMap
        val combineLogFieldsBroadCast = spark.sparkContext.broadcast(combineLogFields)

        val combineLogNeedFields = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/combine-log-need-fields.txt"))
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

        val appStatInstallRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/ratefeature/app-install-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val appStatOpenTimeRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/ratefeature/app-open-time-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val minMaxStatistics = MinMaxStatistics.getMinMaxStatistics(spark, args("minMax"))
        val minMaxStatisticsBroadCast = spark.sparkContext.broadcast(minMaxStatistics)

        val tDF = spark.read.parquet(args("input"))
            .repartition(100)
            .as[UALProcessed]
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)

//                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 0)

//                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 6)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryDetailRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryStatRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDurationRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageDayRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appUsageTimeRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatInstallRateBroadCast.value)(MergedMethod.avg)

                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appStatOpenTimeRateBroadCast.value)(MergedMethod.avg)

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

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        lrFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.toSeq.map(_._2)
                k -> mergedMethod(vss)
            }
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addOneHotFeature(startIndex, 0, lrFields(index), Discretization.minMax(minMaxMap(index).min, minMaxMap(index).max, value))
            }

        startIndex + lrFields.size * 5
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrLogFields: Set[Int], combineLogFields: Map[String, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        lrLogFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.toSeq.map(_._2)
                k -> mergedMethod(vss)
            }

        actionSeq
            .keys
            .toSeq
            .sorted
            .combinations(2)
            .filter { a =>
                val key = a.head + "," + a.last
                combineLogFields.contains(key)
            }
            .foreach { a =>
                val key = a.head + "," + a.last
                val fi = Discretization.minMax(minMaxMap(a.head).min, minMaxMap(a.head).max, actionSeq(a.head))
                val se = Discretization.minMax(minMaxMap(a.last).min, minMaxMap(a.last).max, actionSeq(a.last))
                val value = fi * se

                featureBuilder.addOneHotFeature(startIndex, 0, combineLogFields(key), if(value == 0.0) 0.0 else Math.log(value))
            }

        startIndex + combineLogFields.size * 5
    }

    def encodeRateFeature(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, rateMap: Map[Int, Int])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionMap = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        rateMap.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.toSeq.map(_._2)
                k -> mergedMethod(vss)
            }

        val sum = actionMap.values.sum

        actionMap
            .map { case(id, value) =>
                id -> value / sum
            }
            .toSeq
            .sortBy(_._1)
            .foreach { case(id, value) =>
                featureBuilder.addOneHotFeature(startIndex, 0, rateMap(id), value)
            }

        startIndex + rateMap.size * 5
    }
}

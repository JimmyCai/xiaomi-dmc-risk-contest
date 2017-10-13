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

        val needFields = Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/lr-fields.txt"))
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

        val minMaxStatistics = MinMaxStatistics.getMinMaxStatistics(spark, args("minMax"))
        val minMaxStatisticsBroadCast = spark.sparkContext.broadcast(minMaxStatistics)

//        val positiveVector = Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/positive_vector"))
//            .getLines()
//            .map { line =>
//                val split = line.split("\t")
//                split.head.toInt -> split.last.toDouble
//            }
//            .toSeq
//
//        val positiveSum = Math.sqrt(positiveVector.map(k => Math.pow(k._2, 2)).sum)
//        val positiveVectorNormalized = positiveVector
//            .map { case(id, value) =>
//                id -> value/positiveSum
//            }
//            .toMap
//        val positiveVectorBroadCast = spark.sparkContext.broadcast(positiveVectorNormalized)
//
//        val appInstallRate = Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/app_install_rate.txt"))
//            .getLines()
//            .map { line =>
//                val split = line.split("\t")
//                split.head.toInt -> split.last.toInt
//            }
//            .toMap
//        val appInstallRateBroadCast = spark.sparkContext.broadcast(appInstallRate)
//
//        val appOpenTimeRateBroadCast = spark.sparkContext.broadcast(
//            appInstallRate
//                .map { case(id, index) =>
//                    id + 1 -> index
//                }
//        )
//
//        val queryStatRateBroadCast = spark.sparkContext.broadcast(
//            (10131 to 10233)
//                .zipWithIndex
//                .toMap
//        )

        val tDF = spark.read.parquet(args("input"))
            .repartition(100)
            .as[UALProcessed]
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 0)(MergedMethod.avg)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, 6)(MergedMethod.avg)

                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 0)

                startIndex = MissingValue.encodeLR(featureBuilder, ual, startIndex, 12)

//                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appInstallRateBroadCast.value)(MergedMethod.avg)
//
//                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, appOpenTimeRateBroadCast.value)(MergedMethod.avg)
//
//                startIndex = encodeRateFeature(featureBuilder, ual, startIndex, queryStatRateBroadCast.value)(MergedMethod.avg)
//
//                startIndex = encodePositiveCosine(featureBuilder, ual, startIndex, positiveVectorBroadCast.value)(MergedMethod.avg)

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

    def encodeCombineFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Set[Int], combineFields: Map[String, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
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

        actionSeq
            .keys
            .toSeq
            .sorted
            .combinations(2)
            .filter { a =>
                val key1 = a.head + "," + a.last
                val key2 = a.last + "," + a.head
                combineFields.contains(key1) || combineFields.contains(key2)
            }
            .foreach { a =>
                val key1 = a.head + "," + a.last
                val key2 = a.last + "," + a.head
                val fi = Discretization.minMax(minMaxMap(a.head).min, minMaxMap(a.head).max, actionSeq(a.head))
                val se = Discretization.minMax(minMaxMap(a.last).min, minMaxMap(a.last).max, actionSeq(a.last))
                if(combineFields.contains(key1)) {
                    featureBuilder.addFeature(startIndex, 0, combineFields(key1), if(se == 0.0) 0.0 else fi / se)
                }

                if(combineFields.contains(key2)) {
                    featureBuilder.addFeature(startIndex, 0, combineFields(key2), if(fi == 0.0) 0.0 else se / fi)
                }
            }

        startIndex + combineFields.size
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

    def encodePositiveCosine(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, vectorMap: Map[Int, Double])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionMap = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        vectorMap.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.toSeq.map(_._2)
                k -> mergedMethod(vss)
            }

        val actionSum = Math.sqrt(actionMap.map(k => Math.pow(k._2, 2)).sum)
        val actionMapNormalized = actionMap
            .map { case(id, value) =>
                id -> value/actionSum
            }

        val cosSim = vectorMap
            .map { case (id, value) =>
                Try(actionMapNormalized(id) * value).getOrElse(0.0)
            }
            .sum

        val fv = if(cosSim.toString == "NaN") 0.0 else cosSim

        featureBuilder.addOneHotFeature(startIndex, 1, 0, Discretization.minMax(0.01, 0.5550, fv))
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

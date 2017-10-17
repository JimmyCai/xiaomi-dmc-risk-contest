package com.xiaomi.ad.feature_engineering

import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.MinMax
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Try

/**
  * Created by limingcai on 2017/10/15.
  */
object FeatureTmp {
    def rateFeature(spark: SparkSession) = {
        val queryDetailRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ratefeature/query-detail-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val queryDetailFieldBroadCast = spark.sparkContext.broadcast(
            (131 to 10130).toSet
        )

        val queryStatRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ratefeature/query-stat-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val queryStatFieldBroadCast = spark.sparkContext.broadcast(
            (10131 to 10233).toSet
        )

        val appUsageDurationRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ratefeature/app-usage-duration-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val appUsageDurationFieldsBroadCast = spark.sparkContext.broadcast(
            (10234 to 40180).toSet
        )

        val appUsageDayRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ratefeature/app-usage-day-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val appUsageDayFieldsBroadCast = spark.sparkContext.broadcast(
            (40181 to 68114).toSet
        )

        val appUsageTimeRateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ratefeature/app-usage-time-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )

        val appUsageTimeFieldsBroadCast = spark.sparkContext.broadcast(
            (68115 to 96048).toSet
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

        val appStatInstallFieldsBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/ratefeature/app-install-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt
                }
                .toSet
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

        val appStatOpenTimeFieldsBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(LRFeature.getClass.getResourceAsStream("/ratefeature/app-open-time-rate-fields.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt
                }
                .toSet
        )

        val tsAvgFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ts-avg-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val tsAvgBroadCast = spark.sparkContext.broadcast(tsAvgFields)

        val tsMaxFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/ts-max-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val tsMaxBroadCast = spark.sparkContext.broadcast(tsMaxFields)

        val combineFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/combine-need-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .toMap
        val combineFieldsBroadCast = spark.sparkContext.broadcast(combineFields)

        val combineNeedFields = Source.fromInputStream(LightGBMFeature.getClass.getResourceAsStream("/combine-need-fields.txt"))
            .getLines()
            .flatMap { line =>
                val split = line.split("\t")
                val ss = split.head.split(",")
                Seq(ss.head.toInt, ss.last.toInt)
            }
            .toSet
        val combineNeedFieldsBroadCast = spark.sparkContext.broadcast(combineNeedFields)
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

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbFields: Map[Int, Int], minMaxMap: Map[Int, MinMax], valueMedianMap: Map[Int, Double])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeqRow = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        xgbFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.toSeq.map(_._2)
                val mergedValue = mergedMethod(vss)
                val minMax = minMaxMap(k)
                val finalV = if(mergedValue < minMax.min) minMax.min else if(mergedValue > minMax.max) minMax.max else mergedValue
                k -> finalV
            }

        val actionSeqFillUp = valueMedianMap
            .filter { case(id, value) =>
                !actionSeqRow.contains(id)
            }

        val actionSeq = (actionSeqRow ++ actionSeqFillUp)
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addFeature(startIndex, 0, xgbFields(index), value)
            }

        startIndex + xgbFields.size
    }

    def encodeCombineFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbFields: Set[Int], combineFields: Map[String, Int])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        xgbFields.contains(index)
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

                if(combineFields.contains(key1)) {
                    val value = if(actionSeq(a.last) != 0.0) actionSeq(a.head) / actionSeq(a.last) else 0.0
                    featureBuilder.addFeature(startIndex, 0, combineFields(key1), value)
                }

                if(combineFields.contains(key2)) {
                    val value = if(actionSeq(a.head) != 0.0) actionSeq(a.last) / actionSeq(a.head) else 0.0
                    featureBuilder.addFeature(startIndex, 0, combineFields(key2), value)
                }
            }

        startIndex + combineFields.size
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbLogFields: Set[Int], combineLogFields: Map[String, Int])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        xgbLogFields.contains(index)
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
                val value = actionSeq(a.head) * actionSeq(a.last)

                featureBuilder.addFeature(startIndex, 0, combineLogFields(key), if(value == 0.0) 0.0 else Math.log(value))
            }

        startIndex + combineLogFields.size
    }
}

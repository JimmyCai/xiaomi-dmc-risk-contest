package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.{MinMax, MinMaxStatistics}
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-9-28.
  */
object XGBFeature {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val needFields = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/xgb-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val needFieldsBroadCast = spark.sparkContext.broadcast(needFields)

        val valueMedian = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/xgb-value-median.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toDouble
            }
            .toMap
        val valueMedianBroadCast = spark.sparkContext.broadcast(valueMedian)

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

        val outUser = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/out_user.txt"))
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

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value, valueMedianBroadCast.value)(MergedMethod.avg)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.max)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex)

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

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbFields: Map[Int, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
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
                val mergedValue = mergedMethod(vss)
                val minMax = minMaxMap(k)
                val finalV = if(mergedValue < minMax.min) minMax.min else if(mergedValue > minMax.max) minMax.max else mergedValue
                k -> finalV
            }
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

package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.MinMaxStatistics
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

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

        val combineFields = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/combine-need-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .toMap
        val combineFieldsBroadCast = spark.sparkContext.broadcast(combineFields)

        val combineNeedFields = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/combine-need-fields.txt"))
            .getLines()
            .flatMap { line =>
                val split = line.split("\t")
                val ss = split.head.split(",")
                Seq(ss.head.toInt, ss.last.toInt)
            }
            .toSet
        val combineNeedFieldsBroadCast = spark.sparkContext.broadcast(combineNeedFields)

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

        val outUser = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/out_user.txt"))
            .getLines()
            .map { line =>
                line.split("\t").head
            }
            .toSeq
        val outUserBroadCast = spark.sparkContext.broadcast(outUser)

        val minMaxStatistics = MinMaxStatistics.getMinMaxStatistics(spark, args("minMax"))
        val minMaxStatisticsBroadCast = spark.sparkContext.broadcast(minMaxStatistics)

        val tDF = spark.read.parquet(args("input"))
            .as[UALProcessed]
            .filter { ual =>
                !outUserBroadCast.value.contains(ual.user)
            }
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = encodeCombineFeatures(featureBuilder, ual, startIndex, combineNeedFieldsBroadCast.value, combineFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = MissingValue.encode(featureBuilder, ual, startIndex)

                (ual.user, startIndex - 1, ual.label + featureBuilder.getFeature())
            }


        val ansDF = tDF.orderBy($"_1")
            .map { r =>
                r._1 + "\t" + r._2 + "\t" + r._3
            }

        ansDF
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Map[Int, Int], minMaxMap: Map[Int, (Double, Double)])(implicit mergedMethod: Seq[Double] => Double) = {
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
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addFeature(startIndex, 0, lrFields(index), Discretization.minMax(minMaxMap(index)._1, minMaxMap(index)._2, value))
            }

        startIndex + lrFields.size
    }

    def encodeCombineFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Set[Int], combineFields: Map[String, Int], minMaxMap: Map[Int, (Double, Double)])(implicit mergedMethod: Seq[Double] => Double) = {
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
                val fi = Discretization.minMax(minMaxMap(a.head)._1, minMaxMap(a.head)._2, actionSeq(a.head))
                val se = Discretization.minMax(minMaxMap(a.last)._1, minMaxMap(a.last)._2, actionSeq(a.last))
                if(combineFields.contains(key1)) {
                    featureBuilder.addFeature(startIndex, 0, combineFields(key1), if(se == 0.0) 0.0 else fi / se)
                }

                if(combineFields.contains(key2)) {
                    featureBuilder.addFeature(startIndex, 0, combineFields(key2), if(fi == 0.0) 0.0 else se / fi)
                }
            }

        startIndex + combineFields.size
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrLogFields: Set[Int], combineLogFields: Map[String, Int], minMaxMap: Map[Int, (Double, Double)])(implicit mergedMethod: Seq[Double] => Double) = {
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
                val fi = Discretization.minMax(minMaxMap(a.head)._1, minMaxMap(a.head)._2, actionSeq(a.head))
                val se = Discretization.minMax(minMaxMap(a.last)._1, minMaxMap(a.last)._2, actionSeq(a.last))
                val value = fi * se

                featureBuilder.addFeature(startIndex, 0, combineLogFields(key), if(value == 0.0) 0.0 else Math.log(value))
            }

        startIndex + combineLogFields.size
    }
}

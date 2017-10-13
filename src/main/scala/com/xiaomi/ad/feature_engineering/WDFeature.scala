package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.{MinMax, MinMaxStatistics}
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-10-11.
  *
  * basic profile: 1 - 432
  * wide feature: 433 - 7167 : 6735
  * wide com log feature: 7168 - 9167 : 2000
  * deep feature: 9168 - 72771 : 63604
  */
object WDFeature {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val wideNeedFields = Source.fromInputStream(WDFeature.getClass.getResourceAsStream("/xgb-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val wideNeedFieldsBroadCast = spark.sparkContext.broadcast(wideNeedFields)

        val deepNeedFields = Source.fromInputStream(WDFeature.getClass.getResourceAsStream("/deep-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toInt
            }
            .toMap
        val deepNeedFieldsBroadCast = spark.sparkContext.broadcast(deepNeedFields)

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

        val tDF = spark.read.parquet(args("input"))
            .repartition(200)
            .as[UALProcessed]
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

//                startIndex = encodeFeatures(featureBuilder, ual, startIndex, wideNeedFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)
//
//                startIndex = encodeCombineLogFeatures(featureBuilder, ual, startIndex, combineLogNeedFieldsBroadCast.value, combineLogFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

                startIndex = encodeDeepFeatures(featureBuilder, ual, startIndex, deepNeedFieldsBroadCast.value, minMaxStatisticsBroadCast.value)(MergedMethod.avg)

//                FeatureEncoded(ual.user, startIndex - 1, ual.label + "," + featureBuilder.getCSVFeature(startIndex - 1))
                FeatureEncoded(ual.user, startIndex - 1, ual.label + featureBuilder.getFeature())
            }

//        val featureSize = tDF.take(1).head.featureSize
//        val featureHeadStr = (1 to featureSize)
//            .map { i =>
//                if(i <= 432) s"basic_$i"
//                else if(i <= 9167) s"wide_$i"
//                else s"deep_$i"
//            }
//            .mkString(",")
//        val headStr = s"user_id,label,$featureHeadStr"
//
//        val headerDF = spark.sparkContext.parallelize(Seq(headStr)).toDF().as[String]
//
//        val ansDF = tDF.orderBy($"user")
//            .map { r =>
//                r.user + "," + r.features
//            }
//
//        headerDF.union(ansDF)
//            .write
//            .mode(SaveMode.Overwrite)
//            .text(args("output"))

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

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, wideFields: Map[Int, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        wideFields.contains(index)
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
                featureBuilder.addOneHotFeature(startIndex, 0, wideFields(index), Discretization.minMax(minMaxMap(index).min, minMaxMap(index).max, value))
            }

        startIndex + wideFields.size * 5
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, wideLogFields: Set[Int], combineLogFields: Map[String, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        wideLogFields.contains(index)
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

    def encodeDeepFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, deepFields: Map[Int, Int], minMaxMap: Map[Int, MinMax])(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .values
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        deepFields.contains(index)
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
                featureBuilder.addFeature(startIndex, 0, deepFields(index), Discretization.minMax(minMaxMap(index).min, minMaxMap(index).max, value))
            }

        startIndex + deepFields.size
    }
}

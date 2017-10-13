package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.{MinMax, MinMaxStatistics}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

/**
  * Created by cailiming on 17-10-13.
  */
object LSTMFeature {
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
                !outUserBroadCast.value.contains(ual.user) && ual.actions.size == 12
            }
            .flatMap { ual =>
                assert(ual.actions.size == 12)

                ual.actions
                    .map { case(time, ualAction) =>
                        val featureBuilder = new FeatureBuilder
                        var startIndex = 1

                        startIndex = encodeFeatures(featureBuilder, ualAction, startIndex, needFieldsBroadCast.value, minMaxStatisticsBroadCast.value)

                        startIndex = MissingValue.encodeOneMonth(featureBuilder, ualAction, startIndex)

                        LSTMFeatureEncoded(ual.user, startIndex - 1, time, ual.label + featureBuilder.getFeature())
                    }
            }

        val ansDF = tDF.orderBy($"user", $"time")
            .map { r =>
                r.user + "\t" + r.featureSize + "\t" + r.time + "\t" + r.features
            }

        ansDF
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def encodeFeatures(featureBuilder: FeatureBuilder, ualAction: scala.collection.Map[Int, Double], startIndex: Int, xgbFields: Map[Int, Int], minMaxMap: Map[Int, MinMax]) = {
        val actionSeq = ualAction
            .filter(a => xgbFields.contains(a._1))
            .map { case(id, value) =>
                val minMax = minMaxMap(id)
                val finalV = if(value < minMax.min) minMax.min else if(value > minMax.max) minMax.max else value
                id -> finalV
            }
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addFeature(startIndex, 0, xgbFields(index), Discretization.softSign(value))
            }

        startIndex + xgbFields.size
    }
}

package com.xiaomi.ad.feature_engineering

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
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

        val outUser = Source.fromInputStream(XGBFeature.getClass.getResourceAsStream("/out_user.txt"))
            .getLines()
            .map { line =>
                line.split("\t").head
            }
            .toSeq
        val outUserBroadCast = spark.sparkContext.broadcast(outUser)

        val tDF = spark.read.parquet(args("input"))
            .as[UALProcessed]
            .filter { ual =>
                !outUserBroadCast.value.contains(ual.user)
            }
            .map { ual =>
                val featureBuilder = new FeatureBuilder
                var startIndex = 1

                startIndex = BasicProfile.encode(featureBuilder, ual, startIndex)

                startIndex = encodeFeatures(featureBuilder, ual, startIndex, needFieldsBroadCast.value)(MergedMethod.avg)

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

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Map[Int, Int])(implicit mergedMethod: Seq[Double] => Double) = {
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
                featureBuilder.addFeature(startIndex, 0, lrFields(index), Discretization.softSign(value))
            }

        startIndex + lrFields.size
    }
}

package com.xiaomi.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by cailiming on 17-10-9.
  */
object PositiveVector {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val xgbFields = Source.fromInputStream(PositiveVector.getClass.getResourceAsStream("/xgb-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt
            }
            .toSet

        val xgbFieldsBroadCast = spark.sparkContext.broadcast(xgbFields)

        val ans = spark.read.parquet(args("input"))
            .repartition(100)
            .as[UALProcessed]
            .filter(_.label == 1)
            .flatMap { ual =>
                ual.actions
                    .values
                    .filter(_.nonEmpty)
                    .flatMap { curAction =>
                        curAction
                            .filter { case(index, _) =>
                                xgbFieldsBroadCast.value.contains(index)
                            }
                    }
                    .groupBy(_._1)
                    .map { case (k, vs) =>
                        val vss = vs.toSeq.map(_._2)
                        k -> MergedMethod.avg(vss)
                    }
            }
            .groupBy($"_1".alias("id"))
            .agg(avg($"_2").alias("value"))
            .map { r =>
                val id = r.getAs[Int]("id")
                val value = r.getAs[Double]("value")
                f"$id\t$value%1.4f"
            }

        ans.repartition(1).write.mode(SaveMode.Overwrite).text(args("output"))

        spark.stop()
    }
}

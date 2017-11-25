package com.xiaomi.miui.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.miui.ad.others.UALProcessed
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-10-17.
  */
object MaxChangeMinMaxStatistics {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        spark.read.parquet(args("input"))
            .as[UALProcessed]
            .flatMap { ual =>
                ual.actions
                    .values
                    .filter(_.nonEmpty)
                    .flatMap { curAction =>
                        curAction
                    }
                    .groupBy(_._1)
                    .map { case (k, vs) =>
                        val vss = vs.toSeq.map(_._2)
                        val change = vss.max - vss.min
                        k -> change
                    }
            }
            .groupByKey(_._1)
            .mapGroups { case(k, vs) =>
                val vsSeq = vs.toSeq
                val sortedVs = vsSeq.map(_._2).sorted

                val start = Math.floor(sortedVs.size * 0.05).toInt
                val end = Math.floor(sortedVs.size * 0.95).toInt
                assert(start <= end)
                assert(sortedVs(start) <= sortedVs(end))
                f"$k\t${sortedVs(start)}%1.4f\t${sortedVs(end)}%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def getMinMaxStatistics(spark: SparkSession, path: String) = {
        import spark.implicits._

        spark.read.text(path)
            .as[String]
            .map { line =>
                val splits = line.split("\t")
                splits.head.toInt -> MinMax(splits(1).toDouble, splits(2).toDouble)
            }
            .collect()
            .filter { case(id, minMax) =>
                minMax.max != minMax.min
            }
            .toMap
    }
}

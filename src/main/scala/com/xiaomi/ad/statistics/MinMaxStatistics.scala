package com.xiaomi.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.tools.MergedMethod
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-30.
  */
object MinMaxStatistics {
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
                        k -> MergedMethod.avg(vss)
                    }
            }
            .groupByKey(_._1)
            .mapGroups { case(k, vs) =>
                val vsSeq = vs.toSeq
                val sortedVs = vsSeq.map(_._2).sorted
                sortedVs.zipWithIndex.drop(1)
                    .foreach { case(cv, i) =>
                        assert(cv >= sortedVs(i - 1))
                    }

                val start = Math.floor(sortedVs.size * 0.05).toInt
                val end = Math.floor(sortedVs.size * 0.95).toInt
                assert(start <= end)
                f"$k\t${sortedVs(start)%1.4f}\t${sortedVs(end)%1.4f}"
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
                splits.head.toInt -> (splits(1).toDouble, splits(2).toDouble)
            }
            .collect()
            .toMap
    }
}

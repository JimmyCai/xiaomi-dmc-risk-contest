package com.xiaomi.miui.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.miui.ad.others.UALProcessed
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-28.
  */
object MissingColumnStatistics {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val inputDF = spark.read.parquet(args("input"))
            .as[UALProcessed]

        val allCount = inputDF.count()

        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)
        val categorySeqBroadCast = spark.sparkContext.broadcast(categorySeq)

        val cntDF = inputDF
            .flatMap { ual =>
                val curActions = ual.actions
                    .values
                    .flatMap { ca =>
                        ca
                    }
                    .filter { c =>
                        categorySeqBroadCast.value.contains(c._1) || c._2 > 0.0
                    }
                    .map(_._1)
                    .toSet

                (1 to 96116)
                    .filter(i => !curActions.contains(i))
                    .map(i => i -> 1)
            }
            .groupBy($"_1".alias("field"))
            .count()

        cntDF
            .orderBy($"count")
            .map { row =>
                val field = row.getAs[Int]("field")
                val count = row.getAs[Long]("count")

                val missV = (allCount - count) * 1.0 / allCount
                f"$field\t$missV%1.4f"
            }
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}

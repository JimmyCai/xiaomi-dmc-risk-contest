package com.xiaomi.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-28.
  */
object MissingRowStatistics {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val inputDF = spark.read.parquet(args("input"))
            .as[UALProcessed]

        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)
        val categorySeqBroadCast = spark.sparkContext.broadcast(categorySeq)

        inputDF
            .map { ual =>
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

                val missV = curActions.size * 1.0 / 96116

                ual.user -> missV
            }
            .orderBy($"_2".desc)
            .map { row =>
                f"${row._1}\t${row._2}%1.4f"
            }
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}

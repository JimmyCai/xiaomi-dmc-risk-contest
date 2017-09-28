package com.xiaomi.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 17-9-28.
  */
object CategoryRateStatistics {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val targetField = args("target").toInt

        spark.read.parquet(args("input"))
            .as[UALProcessed]
            .map { ual =>
                val curActions = ual.actions
                    .values
                    .flatMap { ca =>
                        ca
                    }
                    .filter(_._1 == targetField)
                    .toSeq

                val v = if(curActions.isEmpty) -1.0 else curActions.head._2

                v -> ual.label
            }
            .filter($"_1" >= 0.0)
            .groupBy($"_1".alias("value"))
            .agg(avg($"_2").alias("rate"))
            .orderBy($"rate".desc)
            .collect()
            .foreach(println)

        spark.stop()
    }
}

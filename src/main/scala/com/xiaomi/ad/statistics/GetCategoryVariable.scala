package com.xiaomi.ad.statistics

import com.twitter.scalding.Args
import com.xiaomi.ad.others.UALProcessed
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 17-10-18.
  */
object GetCategoryVariable {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val field = args("field").toInt

        val ans = spark.read.parquet(args("input"))
            .as[UALProcessed]
            .flatMap { ual =>
                ual.actions
                    .values
                    .filter(_.nonEmpty)
                    .flatMap { curAction =>
                        curAction
                    }
                    .filter(_._1 == field)
                    .map(_._2)
                    .toSeq
                    .distinct
            }
            .distinct()
            .collect()
            .sorted

        println("------------------------------")
        ans
            .foreach(println)

        println("------------------------------")

        spark.stop()
    }
}

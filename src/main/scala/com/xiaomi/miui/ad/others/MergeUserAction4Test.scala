package com.xiaomi.miui.ad.others

import com.twitter.scalding.Args
import com.xiaomi.miui.ad.tools.MonthTools
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-28.
  */
object MergeUserAction4Test {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val userActionDF = spark.read.text(args("userAction"))
            .as[String]
            .map{ line =>
                val splits = line.split("\t", 3)
                val actions = splits(2)

                UserActions(splits.head, splits(1), actions)
            }
            .select($"user", lit("2017-05").alias("timeLabel"), $"timeAction", $"actions", lit(0).alias("label"))

        val joinDF = userActionDF
            .as[UserActionLabel]
            .groupByKey(_.user)
            .mapGroups{ case(user, actions) =>
                val actionSeq = actions.toSeq
                val labelTime = actionSeq.head.timeLabel
                val actionMap = actionSeq
                    .filter { ua =>
                        ua.timeAction != null && MonthTools.monthDistance(labelTime, ua.timeAction) <= 12
                    }
                    .map{ ua =>
                        val caMap = ua.actions
                            .split(",")
                            .filter(_.nonEmpty)
                            .map { oa =>
                                val cs = oa.split(":")
                                cs.head.toInt -> cs.last.toDouble
                            }
                            .toMap

                        ua.timeAction -> caMap
                    }
                    .toMap

                val fillUpMap = (1 to 12)
                    .filter(m => !actionMap.contains(MonthTools.getDistanceMonth(labelTime, m)))
                    .map(m => MonthTools.getDistanceMonth(labelTime, m) -> Map[Int, Double]())
                    .toMap

                UALProcessed(user, labelTime, actionMap ++ fillUpMap, actionSeq.head.label)
            }

        joinDF
            .repartition(10)
            .write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(args("output"))

        spark.stop()
    }
}

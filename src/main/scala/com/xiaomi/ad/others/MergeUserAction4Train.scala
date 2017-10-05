package com.xiaomi.ad.others

import com.twitter.scalding.Args
import com.xiaomi.ad.tools.MonthTools
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object MergeUserAction4Train {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val labelDF = spark.read.text(args("label"))
            .as[String]
            .map{ line =>
                val splits = line.split("\t", 3)
                Label(splits.head, splits(1), splits(2).toInt)
            }

        val userActionDF = spark.read.text(args("userAction"))
            .as[String]
            .map{ line =>
                val splits = line.split("\t", 3)
                val actions = splits(2)

                UserActions(splits.head, splits(1), actions)
            }

        val joinDF = labelDF.join(userActionDF, Seq("user"), "LEFT")
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

        val positiveValidateDF = joinDF
            .filter(_.label == 1)
            .sample(false, 0.1)

        val negativeValidateDF = joinDF
            .filter(_.label == 0)
            .sample(false, 0.1)

        val validateDF = positiveValidateDF.union(negativeValidateDF).repartition(1)

        val validateUserBroadCast = spark.sparkContext.broadcast(
            validateDF
                .select($"user")
                .as[String]
                .collect()
                .toSet
        )

        val trainDF = joinDF
            .filter { ual =>
                !validateUserBroadCast.value.contains(ual.user)
            }
            .repartition(10)

        validateDF.write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(args("output") + "/validate")

        trainDF.write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(args("output") + "/train")

        spark.stop()
    }
}
package com.xiaomi.ad.tools

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by cailiming on 17-10-17.
  */
object FeatureEncodingTools {
    def getBroadCastFieldMap(spark: SparkSession, path: String) = {
        spark.sparkContext.broadcast(
            Source.fromInputStream(FeatureEncodingTools.getClass.getResourceAsStream(path))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split.last.toInt
                }
                .toMap
        )
    }

    def getBroadCastFields(spark: SparkSession, start: Int, end: Int) = {
        spark.sparkContext.broadcast((start to end).toSet)
    }

    def getBroadCastFields(spark: SparkSession, path: String) = {
        spark.sparkContext.broadcast(
            Source.fromInputStream(FeatureEncodingTools.getClass.getResourceAsStream(path))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt
                }
                .toSet
        )
    }
}

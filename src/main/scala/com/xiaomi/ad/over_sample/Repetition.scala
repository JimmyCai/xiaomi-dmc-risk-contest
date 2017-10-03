package com.xiaomi.ad.over_sample

import java.security.MessageDigest

import com.twitter.scalding.Args
import com.xiaomi.ad.feature_engineering.FeatureEncoded
import org.apache.avro.SchemaBuilder.ArrayBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by limingcai on 2017/10/3.
  */
object Repetition {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val inputDF = spark.read.text(args("input")).as[String]

        val positiveDF = inputDF
            .map { line =>
                val split = line.split("\t")
                FeatureEncoded(split.head, split(1).toInt, split(2))
            }
            .filter { fe =>
                fe.features.split(" ").head == "1"
            }

        val changedPositiveDF = positiveDF
            .map { fe =>
                val ans = fe.copy(user = MD5(fe.user))
                s"${ans.user}\t${ans.featureSize}\t${ans.features}"
            }

        val ansDF = inputDF.union(changedPositiveDF)
            .repartition(10)

        ansDF
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def MD5(s: String) = {
        val m = java.security.MessageDigest.getInstance("MD5")
        val b = s.getBytes("UTF-8")
        m.update(b,0,b.length)
        new java.math.BigInteger(1,m.digest()).toString(16)
    }
}

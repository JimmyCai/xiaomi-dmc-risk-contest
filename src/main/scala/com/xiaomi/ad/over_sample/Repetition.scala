package com.xiaomi.ad.over_sample

import java.security.MessageDigest

import com.twitter.scalding.Args
import com.xiaomi.ad.feature_engineering.FeatureEncoded
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
                val ans = fe.copy(user = md5OfString(fe.user))
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

    private def md5OfString(input: String) = {
        val bytes = MessageDigest.getInstance("MD5").digest(input.getBytes)
        new String(bytes)
    }
}

package com.xiaomi.ad.tools

import org.apache.spark.rdd.RDD

/**
  * Created by cailiming on 17-9-26.
  */
object LibSVMReader {
    def parseLibSVM2GetFeatureSize(rdd: RDD[String]): Int = {
        rdd
            .map{ line =>
                line.split("\t").last
                    .split(" ")
                    .map(cf => cf.split(":").head.toInt)
                    .max
            }
            .max()
    }

    def parseLibSVMRecord(line: String): (String, Double, Array[Int], Array[Double]) = {
        val splits = line.split("\t")
        val (insId, features) =
            if(splits.length == 2) {
                (splits.head, splits(1))
            }
            else {
                (null, splits.head)
            }

        val items = features.split(' ')
        val label = items.head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
            val indexAndValue = item.split(':')
            val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
            (index, value)
        }.unzip

        // check if indices are one-based and in ascending order
        var previous = -1
        var i = 0
        val indicesLength = indices.length
        while (i < indicesLength) {
            val current = indices(i)
            require(current > previous, s"indices should be one-based and in ascending order;"
                + " found current=$current, previous=$previous; line=\"$line\"")
            previous = current
            i += 1
        }
        (insId, label, indices, values)
    }
}

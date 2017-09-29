package com.xiaomi.ad.tools

import org.apache.spark.SparkContext
import breeze.linalg.DenseVector
import com.xiaomi.ad.over_sample.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object LoadSmoteData {

     def readDelimitedData(sc: SparkContext, inputRDD: RDD[String], numFeatures: Int, delimiter: String, numPartitions: Int): RDD[(LabeledPoint,Int,Int)] = {
        val data = inputRDD.filter{x => x.split(delimiter)(0).toDouble == 1.0}.repartition(numPartitions).mapPartitions{x => Iterator(x.toArray)}
        val formatData = data.mapPartitionsWithIndex{(partitionId,iter) =>
            var result = List[(LabeledPoint,Int,Int)]()
            val dataArray = iter.next
            val dataArraySize = dataArray.size - 1
            var rowCount = dataArraySize
            for (i <- 0 to dataArraySize) {
                val parts = dataArray(i).split(delimiter)
                result.::=((LabeledPoint(parts(0).toDouble,DenseVector(parts.slice(1,numFeatures+1)).map(_.toDouble)),partitionId,rowCount))
                rowCount = rowCount - 1
            }
            result.iterator
        }

        formatData
    }

    def readLibSVMData(sc: SparkContext, path: String, numPartitions: Int): RDD[(LabeledPoint,Int,Int)] = {
        val data = sc.textFile(path)
        val numFeatures = LibSVMReader.parseLibSVM2GetFeatureSize(data)

        val tmpRdd = data.map { line =>
            val (id, label, indices, values) = LibSVMReader.parseLibSVMRecord(line)
            val features = Vectors.sparse(numFeatures, indices, values).toDense.values.mkString(",")
            s"$label,$features"
        }

        readDelimitedData(sc, tmpRdd, numFeatures, ",", numPartitions)
    }
    
}

package com.xiaomi.ad.over_sample

import java.security.MessageDigest

import com.twitter.scalding.Args
import com.xiaomi.ad.tools.{LoadSmoteData, NearestNeighbors}
import org.apache.spark.sql.SparkSession

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}

object SMOTE {

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        runSMOTE(spark.sparkContext, args("input"), args("output"), 1.0, 5, 10)

        spark.stop()
    }

    def runSMOTE(sc: SparkContext, 
        inPath: String, 
        outPath: String,
        oversamplingPctg: Double,
        kNN: Int,
        numPartitions: Int): Unit = {

        val rand = new Random()

        val data = LoadSmoteData.readLibSVMData(sc, inPath, numPartitions)
        
        val dataArray = data.mapPartitions(x => Iterator(x.toArray)).cache()

        val numObs = dataArray.map(x => x.size).reduce(_+_)

        println("Number of Filtered Observations "+numObs.toString)        

        val roundPctg = oversamplingPctg
        val sampleData = dataArray.flatMap(x => x).sample(withReplacement = false, fraction = roundPctg, seed = 1L).collect().sortBy(r => (r._2,r._3)) //without Replacement

        println("Sample Data Count "+sampleData.size.toString)

        val globalNearestNeighbors = NearestNeighbors.runNearestNeighbors(dataArray, kNN, sampleData)
        
        val randomNearestNeighbor = globalNearestNeighbors.map(x => (x._1.split(",")(0).toInt,x._1.split(",")(1).toInt,x._2(rand.nextInt(kNN)))).sortBy(r => (r._1,r._2))
        
        val sampleDataNearestNeighbors = randomNearestNeighbor.zip(sampleData).map(x => (x._1._3._1._1, x._1._2, x._1._3._1._2, x._2._1))

        val syntheticData = dataArray.mapPartitionsWithIndex(createSyntheticData(_,_,sampleDataNearestNeighbors)).persist()
        println("Synthetic Data Count "+syntheticData.count.toString)
        val newData = syntheticData
//        val newData = syntheticData.union(sc.textFile(inPath)).repartition(numPartitions)
        println("New Line Count "+newData.count.toString)
        newData.saveAsTextFile(outPath)
    }

    private def createSyntheticData(partitionIndex: Long,
        iter: Iterator[Array[(LabeledPoint,Int,Int)]],
        sampleDataNN: Array[(Int,Int,Int,LabeledPoint)]): Iterator[String]  = {
            
            var result = List[String]()
            val dataArr = iter.next
            val nLocal = dataArr.size - 1            
            val sampleDataNNSize = sampleDataNN.size - 1
            val rand = new Random()            

            for (j <- 0 to sampleDataNNSize){
                val partitionId = sampleDataNN(j)._1
                val neighborId = sampleDataNN(j)._3
                val sampleFeatures = sampleDataNN(j)._4.features
                if (partitionId == partitionIndex.toInt){
                    val currentPoint = dataArr(neighborId)    
                    val features = currentPoint._1.features    
                    sampleFeatures += (sampleFeatures - features) * rand.nextDouble

                    val featureStr = sampleFeatures
                        .toArray
                        .zipWithIndex
                        .filter(_._1 != 0.0)
                        .map { case(f, i) =>
                            f"${i + 1}:$f%1.4f"
                        }
                        .mkString(" ")
                    val featureSize = sampleFeatures.length

                    val user = md5OfString(s"sample$j")
                    result.::=(s"$user\t$featureSize\t1 $featureStr")
                }
            }
        result.iterator
    }

    private def md5OfString(input: String) = {
        val bytes = MessageDigest.getInstance("MD5").digest(input.getBytes)
        new String(bytes)
    }
}

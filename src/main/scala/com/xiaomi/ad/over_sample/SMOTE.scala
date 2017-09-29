package com.xiaomi.ad.over_sample

import com.xiaomi.ad.tools.{NearestNeighbors, LoadSmoteData}

import scala.util.Random
import org.apache.spark.SparkContext

object SMOTE {

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
        val newData = syntheticData.union(sc.textFile(inPath))
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
                        .map { case(f, i) =>
                            f"${i + 1}:$f%1.4f"
                        }
                        .mkString(" ")
                    val featureSize = sampleFeatures.length

                    result.::=(s"sample$j\t$featureSize\t1.0 $featureStr")
                }
            }
        result.iterator
    }        
}

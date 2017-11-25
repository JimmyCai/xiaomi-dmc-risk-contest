package com.xiaomi.miui.ad.feature_engineering

import com.xiaomi.miui.ad.others.UALProcessed
import com.xiaomi.miui.ad.statistics.MinMax

/**
  * Created by cailiming on 17-10-17.
  */
class TreeFeature {
    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbFields: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        xgbFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.map(_._2)
                val mergedValue = mergedMethod(vss)
                val minMax = minMaxMap(k)
                val finalV = if(mergedValue < minMax.min) minMax.min else if(mergedValue > minMax.max) minMax.max else mergedValue
                k -> finalV
            }
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addFeature(startIndex, 0, xgbFields(index), value)
            }

        startIndex + xgbFields.size
    }

    def encodeRateFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, needFields: Set[Int], rateMap: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
        val actionMap = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        needFields.contains(index) && minMaxMap.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.map(_._2)
                val mergedValue = mergedMethod(vss)
                val minMax = minMaxMap(k)
                val finalV = if(mergedValue < minMax.min) minMax.min else if(mergedValue > minMax.max) minMax.max else mergedValue
                k -> finalV
            }

        val sum = actionMap.values.sum

        actionMap
            .filter { case(id, value) =>
                rateMap.contains(id)
            }
            .map { case(id, value) =>
                id -> value / sum
            }
            .toSeq
            .sortBy(_._1)
            .foreach { case(id, value) =>
                featureBuilder.addFeature(startIndex, 0, rateMap(id), value)
            }

        startIndex + rateMap.size
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, xgbLogFields: Set[Int], combineLogFields: Map[String, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
        val actionSeq = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        xgbLogFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.map(_._2)
                val mergedValue = mergedMethod(vss)
                val minMax = minMaxMap(k)
                val finalV = if(mergedValue < minMax.min) minMax.min else if(mergedValue > minMax.max) minMax.max else mergedValue
                k -> finalV
            }

        actionSeq
            .keys
            .toSeq
            .sorted
            .combinations(2)
            .filter { a =>
                val key = a.head + "," + a.last
                combineLogFields.contains(key)
            }
            .foreach { a =>
                val key = a.head + "," + a.last
                val value = actionSeq(a.head) * actionSeq(a.last)

                featureBuilder.addFeature(startIndex, 0, combineLogFields(key), if(value == 0.0) 0.0 else Math.log(value))
            }

        startIndex + combineLogFields.size
    }

    def encodeMaxChangeFeature(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, needFields: Map[Int, Int], month: Int) = {
        val actionSeq = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .filter(_.nonEmpty)
            .flatMap { curAction =>
                curAction
                    .filter { case(index, _) =>
                        needFields.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.map(_._2)
                val change = vss.max - vss.min
                k -> change
            }
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addFeature(startIndex, 0, needFields(index), value)
            }

        startIndex + needFields.size
    }
}

package com.xiaomi.ad.feature_engineering

import com.xiaomi.ad.others.UALProcessed
import com.xiaomi.ad.statistics.MinMax

/**
  * Created by cailiming on 17-10-17.
  */
class OneHotFeature {

    def encodeFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrFields: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
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
                        lrFields.contains(index)
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
                featureBuilder.addOneHotFeature(startIndex, 0, lrFields(index), Discretization.minMax(minMaxMap(index).min, minMaxMap(index).max, value))
            }

        startIndex + lrFields.size * 5
    }

    def encodeCombineFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, needFields: Set[Int], combineFields: Map[String, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
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
                val key1 = a.head + "," + a.last
                val key2 = a.last + "," + a.head
                combineFields.contains(key1) || combineFields.contains(key2)
            }
            .foreach { a =>
                val key1 = a.head + "," + a.last
                val key2 = a.last + "," + a.head

                if(combineFields.contains(key1)) {
                    val fi = Discretization.minMax(minMaxMap(a.head).min, minMaxMap(a.head).max, actionSeq(a.head))
                    val se = Discretization.minMax(minMaxMap(a.last).min, minMaxMap(a.last).max, actionSeq(a.last))
                    val value = if(se != 0.0) fi / se else 0.0
                    featureBuilder.addOneHotFeature(startIndex, 0, combineFields(key1), Discretization.softSign(value))
                }

                if(combineFields.contains(key2)) {
                    val fi = Discretization.minMax(minMaxMap(a.head).min, minMaxMap(a.head).max, actionSeq(a.head))
                    val se = Discretization.minMax(minMaxMap(a.last).min, minMaxMap(a.last).max, actionSeq(a.last))
                    val value = if(fi != 0.0) se / fi else 0.0
                    featureBuilder.addOneHotFeature(startIndex, 0, combineFields(key2), Discretization.softSign(value))
                }
            }

        startIndex + combineFields.size * 5
    }

    def encodeCombineLogFeatures(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, lrLogFields: Set[Int], combineLogFields: Map[String, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
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
                        lrLogFields.contains(index)
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
                val fi = Discretization.minMax(minMaxMap(a.head).min, minMaxMap(a.head).max, actionSeq(a.head))
                val se = Discretization.minMax(minMaxMap(a.last).min, minMaxMap(a.last).max, actionSeq(a.last))
                val value = fi * se

                featureBuilder.addOneHotFeature(startIndex, 0, combineLogFields(key), if(value == 0.0) 0.0 else Math.log(value))
            }

        startIndex + combineLogFields.size * 5
    }

    def encodeRateFeature(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, rateMap: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int)(implicit mergedMethod: Seq[Double] => Double) = {
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
                        rateMap.contains(index) && minMaxMap.contains(index)
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
            .map { case(id, value) =>
                id -> value / sum
            }
            .toSeq
            .sortBy(_._1)
            .foreach { case(id, value) =>
                featureBuilder.addOneHotFeature(startIndex, 0, rateMap(id), value)
            }

        startIndex + rateMap.size * 5
    }

    def encodeTopNRateFeature(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, rateMap: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int, n: Int)(implicit mergedMethod: Seq[Double] => Double) = {
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
                        rateMap.contains(index)
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
            .sortBy(-_._2)
            .take(n)
            .toMap

        val sum = actionMap.values.sum

        actionMap
            .map { case(id, value) =>
                id -> value / sum
            }
            .toSeq
            .sortBy(_._1)
            .foreach { case(id, value) =>
                featureBuilder.addOneHotFeature(startIndex, 0, rateMap(id), value)
            }

        startIndex + rateMap.size * 5
    }

    def encodeMaxChangeFeature(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, needFields: Map[Int, Int], minMaxMap: Map[Int, MinMax], month: Int) = {
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
                        needFields.contains(index) && minMaxMap.contains(index)
                    }
            }
            .groupBy(_._1)
            .map { case (k, vs) =>
                val vss = vs.map(_._2)
                val change = vss.max - vss.min
                val minMax = minMaxMap(k)
                val finalV = if(change < minMax.min) minMax.min else if(change > minMax.max) minMax.max else change
                k -> finalV
            }
            .toSeq
            .sortBy(_._1)

        actionSeq
            .foreach { case(index, value) =>
                featureBuilder.addOneHotFeature(startIndex, 0, needFields(index), Discretization.minMax(minMaxMap(index).min, minMaxMap(index).max, value))
            }

        startIndex + needFields.size * 5
    }
}

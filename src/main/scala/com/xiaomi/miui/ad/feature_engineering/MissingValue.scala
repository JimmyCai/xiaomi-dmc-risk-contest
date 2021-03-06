package com.xiaomi.miui.ad.feature_engineering

import com.xiaomi.miui.ad.others.UALProcessed

/**
  * Created by cailiming on 17-9-28.
  */
object MissingValue {
    val TOTAL_FEATURE = 5000//63614

    def encode(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, month: Int) = {
        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)

        val existIds = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .flatMap { ca =>
                ca
            }
            .filter { c =>
                categorySeq.contains(c._1) || c._2 > 0.0
            }
            .map(_._1)
            .toSet

        val ans = TOTAL_FEATURE - existIds.size
        featureBuilder.addFeature(startIndex, 1, 0, ans)
    }

    def encodeLR(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int, month: Int) = {
        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)

        val existIds = ual.actions
            .toSeq
            .sortBy { case(time, _) =>
                time.replace("-", "").toInt
            }
            .drop(month)
            .map(_._2)
            .flatMap { ca =>
                ca
            }
            .filter { c =>
                categorySeq.contains(c._1) || c._2 > 0.0
            }
            .map(_._1)
            .toSet

        val ans = TOTAL_FEATURE - existIds.size
        featureBuilder.addOneHotFeature(startIndex, 1, 0, Discretization.minMax(10, 5000, ans))
    }

    def encodeOneMonth(featureBuilder: FeatureBuilder, ualAction: scala.collection.Map[Int, Double], startIndex: Int) = {
        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)

        val allSize = ualAction
            .count(a => !categorySeq.contains(a._1))

        featureBuilder.addFeature(startIndex, 1, 0, Discretization.minMax(10, 5000, TOTAL_FEATURE - allSize))
    }
}

package com.xiaomi.ad.feature_engineering

import com.xiaomi.ad.others.UALProcessed

/**
  * Created by cailiming on 17-9-28.
  */
object MissingValue {
    val TOTAL_FEATURE = 63614

    def encode(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int) = {
        val categorySeq = Seq(1, 2, 3, 4, 11, 12, 13, 18, 19, 57, 59)

        val existIds = ual.actions
            .values
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
}

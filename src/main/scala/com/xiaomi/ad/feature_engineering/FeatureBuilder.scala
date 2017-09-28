package com.xiaomi.ad.feature_engineering

/**
  * Created by cailiming on 17-9-14.
  */
class FeatureBuilder {
    val feature = new StringBuilder

    def addFeature(startIndex: Int, featureSize: Int, index: Int, value: Double) = {
        assert(index < featureSize || featureSize == 0)
        if(Math.abs(value - 0.0) <= 0.0001) {
            startIndex + featureSize
        } else {
            val valueStr = if(value.toString.length >= 7) f"$value%1.4f" else value.toString
            feature.append(s" ${startIndex + index}:$valueStr")
            startIndex + featureSize
        }
    }

    def getFeature() = {
        feature.toString()
    }
}

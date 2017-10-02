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

    def addOneHotFeature(startIndex: Int, featureSize: Int, index: Int, value: Double) = {
        assert(index < featureSize || featureSize == 0)
        val oneHotIndex = if(value < 0.2) 0
        else if(value < 0.4) 1
        else if(value < 0.6) 2
        else if(value < 0.8) 3
        else 4

        feature.append(s" ${startIndex + index + oneHotIndex}: 1.0")
        startIndex + (featureSize * 5)
    }

    def getFeature() = {
        feature.toString()
    }
}

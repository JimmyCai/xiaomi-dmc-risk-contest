package com.xiaomi.ad.feature_engineering

/**
  * Created by cailiming on 17-9-28.
  */
object Discretization {
    def softSign(x: Double) = {
        x / (1 + Math.abs(x))
    }

    def minMax(min: Double, max: Double, cur: Double) = {
        if(min == max) cur
        else {
            val t = (cur - min) / (max - min)
            if(t < 0) 0.0
            else if(t > 1) 1.0
            else t
        }
    }
}

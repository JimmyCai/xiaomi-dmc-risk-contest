package com.xiaomi.ad.feature_engineering

/**
  * Created by cailiming on 17-9-28.
  */
object Discretization {
    def softSign(x: Double) = {
        x / (1 + Math.abs(x))
    }
}

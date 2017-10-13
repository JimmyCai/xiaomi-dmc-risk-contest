package com.xiaomi.ad.feature_engineering

/**
  * Created by cailiming on 17-9-29.
  */
case class FeatureEncoded(user: String, featureSize: Int, features: String)
case class LSTMFeatureEncoded(user: String, featureSize: Int, time: String, features: String)

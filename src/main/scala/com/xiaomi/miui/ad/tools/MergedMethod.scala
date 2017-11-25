package com.xiaomi.miui.ad.tools

/**
  * Created by cailiming on 17-9-21.
  */
object MergedMethod {
    def avg(input: Seq[Double]) = {
        input.sum / input.size
    }

    def max(input: Seq[Double]) = {
        input.max
    }

    def min(input: Seq[Double]) = {
        input.min
    }
}

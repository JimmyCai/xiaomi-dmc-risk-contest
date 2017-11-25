package com.xiaomi.miui.ad.tools

/**
  * Created by cailiming on 17-9-28.
  */
object MonthTools {
    def monthDistance(big: String, small: String) = {
        val bigSplit = big.split("-")
        val smallSplit = small.split("-")
        val bigYear = bigSplit.head.toInt
        val bigMonth = bigSplit.last.toInt
        val smallYear = smallSplit.head.toInt
        val smallMonth = smallSplit.last.toInt

        (bigYear - smallYear) * 12 + (bigMonth - smallMonth)
    }

    def getDistanceMonth(target: String, distance: Int) = {
        val year = target.split("-").head.toInt
        val month = target.split("-").last.toInt
        val t = month - distance
        if(t >= 1) year + "-" + getMonthStr(t)
        else (year - 1) + "-" + getMonthStr(12 + t)
    }

    def getMonthStr(m: Int) = {
        if(m < 10) "0" + m else m
    }
}

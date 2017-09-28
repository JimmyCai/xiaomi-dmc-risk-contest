package com.xiaomi.ad.feature_engineering

import scala.util.Try

/**
  * Created by cailiming on 17-9-28.
  */
object BasicProfileFeatureProxy {
    val phoneVersionMap = Map(
        2 -> 1,
        11 -> 2,
        12 -> 3,
        16 -> 4,
        17 -> 5
    )

    def sexProxy(featureBuilder: FeatureBuilder, startIndex: Int, sex: Seq[Int]) = {
        val userSex = sexProxyFirstUnknown(sex)
        featureBuilder.addFeature(startIndex, 3, userSex, 1.0)
    }

    def ageProxy(featureBuilder: FeatureBuilder, startIndex: Int, age: Seq[Int]) = {
        val userAge = ageProxyFirstUnknown(age)
        featureBuilder.addFeature(startIndex, 8, userAge, 1.0)
    }

    def phoneVersionProxy(featureBuilder: FeatureBuilder, startIndex: Int, version: Seq[Int]) = {
        val indexes = version.map(i => phoneVersionMap.getOrElse(i, 0)).distinct.sorted
        indexes
            .map { i =>
                featureBuilder.addFeature(startIndex, 6, i, 1.0)
            }
            .last
    }

    def phoneBigVersionProxy(featureBuilder: FeatureBuilder, startIndex: Int, phoneVersions: Seq[Int]) = {
        val versionsFinal = phoneBigVersionAllIn(phoneVersions)
        versionsFinal
            .map { cv =>
                featureBuilder.addFeature(startIndex, 33, cv, 1.0)
            }
            .last
    }

    def bindProxy(featureBuilder: FeatureBuilder, startIndex: Int, bind: Seq[Int]) = {
        val bp = bindProxyMaxUnknown(bind)
        featureBuilder.addFeature(startIndex, 2, bp, 1.0)
    }

    def provinceProxy(featureBuilder: FeatureBuilder, startIndex: Int, places: Seq[Int]) = {
        val placesFinal = provinceCityProxyAllIn(places)
        placesFinal
            .map { cp =>
                featureBuilder.addFeature(startIndex, 34, cp, 1.0)
            }
            .last
    }

    def cityProxy(featureBuilder: FeatureBuilder, startIndex: Int, places: Seq[Int]) = {
        val placesFinal = provinceCityProxyAllIn(places)
        placesFinal
            .map { cp =>
                featureBuilder.addFeature(startIndex, 342, cp, 1.0)
            }
            .last
    }

    def sexProxyFirstUnknown(sex: Seq[Int]) = {
        (sex.map(s => Try(s).getOrElse(0)) :+ 0).head
    }

    def ageProxyFirstUnknown(age: Seq[Int]) = {
        (age :+ 0).head
    }

    def phoneBigVersionAllIn(phoneVersions: Seq[Int]) = {
        val pVersions = phoneVersions.distinct.sorted
        if(pVersions.isEmpty) Seq(0) else pVersions
    }

    def bindProxyMaxUnknown(bind: Seq[Int]) = {
        (bind.map(v => Try(v).getOrElse(0)) :+ 0).max
    }

    def provinceCityProxyAllIn(places: Seq[Int]) = {
        val allPlaces = places.distinct.sorted
        if(allPlaces.isEmpty) Seq(0) else allPlaces
    }

    def getAgeSeg(age: Int) = {
        val ageI = Try(age).getOrElse(29)
        if(ageI < 18) 1
        else if(ageI < 22) 2
        else if(ageI < 25) 3
        else if(ageI < 30) 4
        else if(ageI < 40) 5
        else if(ageI < 50) 6
        else 7
    }
}

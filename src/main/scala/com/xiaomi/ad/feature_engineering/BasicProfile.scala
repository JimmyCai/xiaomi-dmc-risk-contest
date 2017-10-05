package com.xiaomi.ad.feature_engineering

import com.xiaomi.ad.others.UALProcessed

import scala.util.Try

object BasicProfile {
    def encode(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int) = {
        //性别
        val userSex = getActionSeq(ual, Seq(1, 57))
        val ageStart = BasicProfileFeatureProxy.sexProxy(featureBuilder, startIndex, userSex)

        //年龄
        val userAge = getActionAge(ual)
        val phoneVersionDetailStart = BasicProfileFeatureProxy.ageProxy(featureBuilder, ageStart, userAge)

        //手机的具体型号
        val phoneVersionDetail = getActionSeq(ual, Seq(3))
        val phoneBigVersionStart = BasicProfileFeatureProxy.phoneVersionProxy(featureBuilder, phoneVersionDetailStart, phoneVersionDetail)

        //手机的大型号
        val phoneBigVersion = getActionSeq(ual, Seq(4))
        val bindPhoneStart = BasicProfileFeatureProxy.phoneBigVersionProxy(featureBuilder, phoneBigVersionStart, phoneBigVersion)

        //绑定电话
        val bindPhone = getActionSeq(ual, Seq(11))
        val bindEmailStart = BasicProfileFeatureProxy.bindProxy(featureBuilder, bindPhoneStart, bindPhone)

        //绑定邮箱
        val bindEmail = getActionSeq(ual, Seq(12))
        val bindWeiBoStart = BasicProfileFeatureProxy.bindProxy(featureBuilder, bindEmailStart, bindEmail)

        //绑定微博
        val bindWeiBo = getActionSeq(ual, Seq(13))
        val provinceStart = BasicProfileFeatureProxy.bindProxy(featureBuilder, bindWeiBoStart, bindWeiBo)

        //省份
        val provinces = getActionSeq(ual, Seq(18))
        val cityStart = BasicProfileFeatureProxy.provinceProxy(featureBuilder, provinceStart, provinces)

        //城市
        val cities = getActionSeq(ual, Seq(19))
        BasicProfileFeatureProxy.cityProxy(featureBuilder, cityStart, cities)
    }

    def encodeNonOneHot(featureBuilder: FeatureBuilder, ual: UALProcessed, startIndex: Int) = {
        //性别
        val userSex = getActionSeq(ual, Seq(1, 57))
        val ageStart = featureBuilder.addOneHotFeature(startIndex, 1, 0, Try(userSex.head).getOrElse(0))

        //年龄
        val userAge = getActionAge(ual)
        val phoneVersionDetailStart = featureBuilder.addOneHotFeature(ageStart, 1, 0, Try(userAge.head).getOrElse(0))

        //手机的具体型号
        val phoneVersionDetail = getActionSeq(ual, Seq(3))
        val phoneBigVersionStart = featureBuilder.addOneHotFeature(phoneVersionDetailStart, 1, 0, Try(phoneVersionDetail.head).getOrElse(0))

        //手机的大型号
        val phoneBigVersion = getActionSeq(ual, Seq(4))
        val bindPhoneStart = featureBuilder.addOneHotFeature(phoneBigVersionStart, 1, 0, Try(phoneBigVersion.head).getOrElse(0))
        //绑定电话
        val bindPhone = getActionSeq(ual, Seq(11))
        val bindEmailStart = featureBuilder.addOneHotFeature(bindPhoneStart, 1, 0, Try(bindPhone.head).getOrElse(0))

        //绑定邮箱
        val bindEmail = getActionSeq(ual, Seq(12))
        val bindWeiBoStart = featureBuilder.addOneHotFeature(bindEmailStart, 1, 0, Try(bindEmail.head).getOrElse(0))

        //绑定微博
        val bindWeiBo = getActionSeq(ual, Seq(13))
        val provinceStart = featureBuilder.addOneHotFeature(bindWeiBoStart, 1, 0, Try(bindWeiBo.head).getOrElse(0))

        //省份
        val provinces = getActionSeq(ual, Seq(18))
        val provinceChangedStart = featureBuilder.addOneHotFeature(provinceStart, 1, 0, Try(provinces.head).getOrElse(0))

        val cityStart = featureBuilder.addOneHotFeature(provinceChangedStart, 1, 0, Try(provinces.size).getOrElse(0))

        //城市
        val cities = getActionSeq(ual, Seq(19))
        val cityChangeStart = featureBuilder.addOneHotFeature(cityStart, 1, 0, Try(cities.head).getOrElse(0))

        featureBuilder.addOneHotFeature(cityChangeStart, 1, 0, cities.size)
    }

    def getActionSeq(uALProcessed: UALProcessed, idSeq: Seq[Int]) = {
        uALProcessed
            .actions
            .toSeq
            .sortBy(-_._1.replace("-", "").trim.toInt)
            .flatMap { case(time, action) =>
                action
                    .filter(i => idSeq.contains(i._1))
                    .values
                    .map(_.toInt)
                    .toSeq
            }
    }

    def getActionAge(ual: UALProcessed) = {
        ual.actions
            .toSeq
            .sortBy(-_._1.replace("-", "").trim.toInt)
            .flatMap{ case(time, action) =>
                action
                    .filter{ oa =>
                        Seq(2, 59).contains(oa._1)
                    }
                    .map{ oa =>
                        oa._1 match {
                            case 2 => oa._2.toInt
                            case _ => BasicProfileFeatureProxy.getAgeSeg(oa._2.toInt)
                        }
                    }
            }
    }
}
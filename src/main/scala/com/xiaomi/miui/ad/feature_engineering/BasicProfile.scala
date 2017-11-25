package com.xiaomi.miui.ad.feature_engineering

import com.xiaomi.miui.ad.others.UALProcessed

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
        val userSex = getActionSeq(ual, Seq(1, 57)) :+ 0
        val ageStart = featureBuilder.addFeature(startIndex, 1, 0, userSex.head)

        //年龄
        val userAge = getActionAge(ual) :+ 0
        val phoneVersionDetailStart = featureBuilder.addFeature(ageStart, 1, 0, userAge.head)

        //手机的具体型号
        val phoneVersionDetail = getActionSeq(ual, Seq(3)) :+ 0
        val phoneBigVersionStart = featureBuilder.addFeature(phoneVersionDetailStart, 1, 0, phoneVersionDetail.head)

        //手机的大型号
        val phoneBigVersion = getActionSeq(ual, Seq(4)) :+ 0
        val bindPhoneStart = featureBuilder.addFeature(phoneBigVersionStart, 1, 0, phoneBigVersion.head)
        //绑定电话
        val bindPhone = getActionSeq(ual, Seq(11)) :+ 0
        val bindEmailStart = featureBuilder.addFeature(bindPhoneStart, 1, 0, bindPhone.head)

        //绑定邮箱
        val bindEmail = getActionSeq(ual, Seq(12)) :+ 0
        val bindWeiBoStart = featureBuilder.addFeature(bindEmailStart, 1, 0, bindEmail.head)

        //绑定微博
        val bindWeiBo = getActionSeq(ual, Seq(13)) :+ 0
        val provinceStart = featureBuilder.addFeature(bindWeiBoStart, 1, 0, bindWeiBo.head)

        //省份
        val provinces = getActionSeq(ual, Seq(18)) :+ 0
        val provinceChangedStart = featureBuilder.addFeature(provinceStart, 1, 0, provinces.head)

        val cityStart = featureBuilder.addFeature(provinceChangedStart, 1, 0, provinces.size - 1)

        //城市
        val cities = getActionSeq(ual, Seq(19)) :+ 0
        val cityChangeStart = featureBuilder.addFeature(cityStart, 1, 0, cities.head)

        featureBuilder.addFeature(cityChangeStart, 1, 0, cities.size - 1)
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
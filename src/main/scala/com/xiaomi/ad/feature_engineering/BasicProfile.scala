package com.xiaomi.ad.feature_engineering

import com.xiaomi.ad.others.UALProcessed

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

    def getActionSeq(uALProcessed: UALProcessed, idSeq: Seq[Int]) = {
        uALProcessed
            .actions
            .flatMap { case(time, action) =>
                action
                    .filter(i => idSeq.contains(i._1))
                    .values
                    .map(_.toInt)
                    .toSeq
            }
            .toSeq
    }

    def getActionAge(ual: UALProcessed) = {
        ual.actions
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
            .toSeq
    }
}
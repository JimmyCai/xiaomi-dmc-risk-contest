import com.xiaomi.ad.feature_engineering.{BasicProfile, FeatureBuilder}
import com.xiaomi.ad.others.UALProcessed

/**
  * Created by cailiming on 17-10-9.
  */
object LRTest {
    def main(args: Array[String]): Unit = {
        val featureBuilder = new FeatureBuilder
        val m1 = Map(19 -> 1.0)
        val m2 = Map(19 -> 5.0)
        val ual = UALProcessed("", "", Map("2016-10" -> m2, "2016-11" -> m1), 0)
        val index = BasicProfile.encode(featureBuilder, ual, 1)
        println(index)
        println(featureBuilder.getFeature())
    }
}

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

/**
  * Created by cailiming on 17-9-28.
  */
object XGBNeedFields {
    def main1(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/lr-fields.txt")))

        val xgb = Source.fromInputStream(getClass.getResourceAsStream("/xgb-importance.txt"))
            .getLines()
            .map(l => l.split("\t").head.toInt - 429)
            .toSeq

        val outvar = Source.fromInputStream(getClass.getResourceAsStream("/out_var.txt"))
            .getLines()
            .map(l => l.split("\t").head.toInt)
            .toSeq

        (1 to 96116)
            .filter(i => !outvar.contains(i) || xgb.contains(i))
            .sorted
            .zipWithIndex
            .foreach{ case (i, ii) =>
                bw.write(s"$i\t$ii\n")
            }

        bw.flush()
        bw.close()
    }

    def main2(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/combine-try-fields.txt")))

        val xgb = Source.fromInputStream(getClass.getResourceAsStream("/xgb-importance.txt"))
            .getLines()
            .map { l =>
                val split = l.split("\t")
                split.head.toInt - 429 -> split.last.toInt
            }
            .filter(_._2 >= 9)
            .map(_._1)
            .toSeq
            .sorted

        xgb.combinations(2)
            .map { a =>
                s"${a.head},${a.last}"
            }
            .zipWithIndex
            .foreach { case(v, i) =>
                bw.write(s"$v\t$i\n")
            }

        bw.flush()
        bw.close()
    }

    def main4(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/combine-need-fields.txt")))

        val allMap = Source.fromInputStream(getClass.getResourceAsStream("/combine-try-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.last.toInt -> split.head
            }
            .toMap

        val t = Source.fromInputStream(getClass.getResourceAsStream("/com_features.txt"))
            .getLines()
            .map { line =>
                val split = line.split(",")
                val need = split.head.toInt
                need
            }
            .toSeq
            .sorted

        t
            .map { n =>
                allMap(n)
            }
            .zipWithIndex
            .foreach { case(v, i) =>
                bw.write(s"$v\t$i\n")
            }

        bw.flush()
        bw.close()
    }

    def main3(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/combine-try-fields.txt")))

        val t = Source.fromInputStream(getClass.getResourceAsStream("/xgb-importance.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                val need = split.head.toInt - 429
                need
            }
            .toSeq
            .take(100)
            .sorted

        t.combinations(2)
            .flatMap { case a =>
                val fi = a.head + "," + a.last
                val se = a.last + "," + a.head
                Seq(fi, se)
            }
            .zipWithIndex
            .foreach { case(v, i) =>
                bw.write(s"$v\t$i\n")
            }

        bw.flush()
        bw.close()
    }

    def main5(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/max-fields.txt")))

        val t = Source.fromInputStream(getClass.getResourceAsStream("/lr-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.last.toInt -> split.head.toInt
            }
            .toMap

        Source.fromInputStream(getClass.getResourceAsStream("/max-importance.txt"))
            .getLines()
            .map { line =>
                val split = line.split(",")
                t(split.head.toInt)
            }
            .toSeq
            .sorted
            .zipWithIndex
            .foreach { case(k, i) =>
                bw.write(s"$k\t$i\n")
            }

        bw.flush()
        bw.close()
    }

    def main6(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/xgb-value-median.txt")))

        val t = Source.fromInputStream(getClass.getResourceAsStream("/value-median"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt -> split.last.toDouble
            }
            .toMap

        Source.fromInputStream(getClass.getResourceAsStream("/xgb-importance.txt"))
            .getLines()
            .filter { line =>
                val split = line.split("\t")
                val id = split.head.toInt - 429
                t.contains(id)
            }
            .take(100)
            .map { line =>
                val split = line.split("\t")
                val id = split.head.toInt - 429
                id -> t(id)
            }
            .foreach{ case(id, value) =>
                bw.write(f"$id\t$value%1.4f\n")
            }

        bw.flush()
        bw.close()
    }

    def main7(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/app_stat_rate.txt")))

        Source.fromFile("/home/mi/Documents/contest/dmc_risk_variable_app_stat")
            .getLines()
            .drop(1)
            .zipWithIndex
            .filter { case(id, index) =>
                index % 4 == 2
            }
            .map(_._1)
            .zipWithIndex
            .foreach { case(idStr, index) =>
                val id = idStr.split("\t").head
                bw.write(s"$id\t$index\n")
            }

        bw.flush()
        bw.close()
    }

    def main(args: Array[String]): Unit = {
        val bw1 = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/half-year-avg-fields.txt")))
        val bw2 = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/half-year-max-fields.txt")))

        val tMap = Source.fromInputStream(XGBNeedFields.getClass.getResourceAsStream("/lr-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                (split.last.toInt + 1) -> split.head.toInt
            }
            .toMap

        val features = Source.fromInputStream(XGBNeedFields.getClass.getResourceAsStream("/half-year-feature-score.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head.toInt
            }
        features
            .filter(_ <= 63615)
            .toSeq
            .sorted
            .map(i => tMap(i))
            .zipWithIndex
            .foreach { case(id, index) =>
                bw1.write(s"$id\t$index\n")
            }

        features
            .filter(i => i > 63615 && i <= 127230)
            .toSeq
            .sorted
            .map(i => tMap(i - 63615))
            .zipWithIndex
            .foreach { case(id, index) =>
                bw2.write(s"$id\t$index\n")
            }

        bw1.flush()
        bw2.flush()
        bw1.close()
        bw2.close()
    }
}

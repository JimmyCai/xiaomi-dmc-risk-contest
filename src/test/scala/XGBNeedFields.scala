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

    def main3(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/combine-log-need-fields.txt")))

        val allMap = Source.fromInputStream(getClass.getResourceAsStream("/combine-try-fields.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.last.toInt -> split.head
            }
            .toMap

        val t = Source.fromInputStream(getClass.getResourceAsStream("/com-log-feature.txt"))
            .getLines()
            .map { line =>
                val split = line.split(",")
                val need = split.head.toInt
                need
            }
            .toSeq
            .sorted

        t.foreach(println)

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

    def main(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/Users/limingcai/Desktop/combine-try-fields.txt")))

        val t = Source.fromInputStream(getClass.getResourceAsStream("/xgb-importance.txt"))
            .getLines()
            .map { line =>
                val split = line.split("\t")
                val need = split.head.toInt
                need
            }
            .toSeq
            .take(100)
            .sorted

        println(t.size)

        println(t.combinations(2).size)

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
}

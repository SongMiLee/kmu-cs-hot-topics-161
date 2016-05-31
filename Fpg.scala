import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.FPGrowth

object Fpg{
   def main(args : Array[String]){

        val conf = new SparkConf().setAppName("Fpg")
        val sc = new SparkContext(conf)

        val data = sc.textFile(args(0)) //webdocs.dat path
        val transactions = data.map(x=>x.trim.split(' '))

        val fpg = new FPGrowth()
                  .setMinSupport(0.3) //if minSupport lower than 0.3, it occurs memory fault
                  .setNumPartitions(10)

        val model = fpg.run(transactions)
        model.freqItemsets.map(x=>s"""[${x.items.mkString(",")}], ${x.freq}""")
             .saveAsTextFile(args(1))

   }
}

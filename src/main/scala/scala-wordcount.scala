import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StopWordsRemover

object WordCount {
  def main(args: Array[String]) {
    val removeCharacter = Array()

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    print("input file location is: " + args(0))
    val tweets = sc.textFile(args(0))

    val tweet_words = tweets.flatMap(x=>x.split("\\s+"))

    val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("filtered")

    val len_words = tweet_words.filter(x => x.length()>4) // word length is bigger than 4
    val lower_words = len_words.map(x=>x.toLowerCase()) //make words lower

    val kv = lower_words.map(x => (x, 1))
    val word_count = kv.reduceByKey((x,y) => x+y)
    val top_ten = word_count.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
    top_ten.foreach(println)
/*
    var file = new File(args(1))
    var out = fileSystem.createNewFile(new Path(file.getName))
    out.write(top_ten)
    out.close()*/
  }
}

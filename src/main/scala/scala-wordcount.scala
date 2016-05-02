import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    print("input file location is: " + args(0))
    val tweets = sc.textFile(args(0), 12)

    val tweet_words = tweets.flatMap(x=>x.split("\\s+"))
    val w = Array("`", "-", "=", ";", "'", "/", "~", "!", "@",
		 "#", "$", "%", "^", "&", "|", ":", "<", ">",
		 "\\*", "\\+","\\{","\\}","\\?",".")

    //Stop Words String Array
    val remover = new StopWordsRemover().getStopWords
    var tmp = tweet_words
    var tmp_words = tmp

    //replace stop words to ""
    for(i<- 0 to (remover.length-1)){
      //tmp_words = tmp.filter(x=>(!x.contains(remover(i))))
      tmp_words = tmp.map(x=>x.replace(remover(i),""))
      tmp = tmp_words
    }

  //replace special character
    for(i<-0 to (w.length-1)){
      tmp_words = tmp.map(x=>x.replace(w(i), ""))
      tmp = tmp_words
    }

    val len = tmp.filter(x => x.length()>4) // word length is bigger than 4
    val lower = len.map(x=>x.toLowerCase()) //make words lower

    println(lower.take(10))

    val kv = lower.map(x => (x, 1))
    val word_count = kv.reduceByKey((x,y) => x+y)

    val top_ten = word_count.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
    top_ten.foreach(println)

    val writeRDD = sc.parallelize(top_ten)
    writeRDD.saveAsTextFile(args(1))

  }
}

package com.volkoshkursk.wordcnt

import org.apache.spark.{SparkConf, SparkContext}

object wordcnt {
  def main(args: Array[String]): Unit = {
    // если аргументов нет - вызываем ошибку
    if (args.length == 0) {
      System.err.println("Usage: SparkTest <host> [<slices>]")
      System.exit(1)
    }
    // создаём объект конфигурации и инициализируем Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)
    // если аргумент 1 - просто считаем кол-во слов
    if (args(0) == "1") {
      val file = sc.textFile(args(1))
      val regex = """[^a-zA-Z ]""".r
      val words = file.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
      val counts = words.map(word => (word, 1)).reduceByKey(_ + _)

      counts.foreach(println)
    }
    // если аргумент 2 - считаем кол-во слов, встреченных в 1-м файле
    if (args(0) == "2") {
      val file1 = sc.textFile(args(1))
      val file2 = sc.textFile(args(2))
      val regex = """[^a-zA-Z ]""".r
      val words_1 = file1.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
      val words_2 = file2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" ")).collect()
      val counts = words_1.filter(word=> words_2.contains(word)).map(word=>(word,1)).reduceByKey(_+_).collect()

      counts.foreach(println)
    }
  }
}

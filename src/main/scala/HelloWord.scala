package com.spark.test.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaPairRDD;
import java.lang.System

class MostWordCounter {
    //def wordCounts(spark: SparkSession, fileName: String): RDD[(String,Int)]= {    
    def wordCounts(fileName: String, output: String): Tuple2[String, Int] = {    
        try {
            val start = System.currentTimeMillis()
            println("####--------Scala calling-in time:"+System.currentTimeMillis())
            val spark = SparkSession
            .builder()
            .appName("Spark SQL basic example")
            .getOrCreate()
            val file = spark.sparkContext.textFile(fileName).cache();
            val sortedWordCounts = file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey((a,b)=>a+b).filter(_._1.length>0).sortBy(_._2,false);
            //sortedWordCounts.foreach(x => println("####key:"+x._1+",value:"+x._2))
            println("####--------Scala ended at:"+System.currentTimeMillis())
            println("####--------Scala net time cost:"+(System.currentTimeMillis()-start))
            // Save onto HDFS
            sortedWordCounts.saveAsTextFile(output)
            sortedWordCounts.first()
        } catch { 
            case ex: Exception => {
                println("################# failed:");
                ex.printStackTrace()
                return null;
            } 
        }
    }
}

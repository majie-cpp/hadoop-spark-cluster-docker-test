package com.spark.test.java;

import com.spark.test.scala.*;
import org.apache.spark.sql.SparkSession;
import scala.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD; 
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class WordCountTest {
    private static final Pattern SPACE = Pattern.compile(" ");
    public static void main(String[] args) {
        long start1 = System.currentTimeMillis() ;
        long callingDoneTime = 0;
        System.out.println("####---------- Before calling Scala:"+System.currentTimeMillis());
        MostWordCounter wc = new MostWordCounter(); 
        Tuple2<String,Object>  mostWord = wc.wordCounts(args[0],args[1]);
        System.out.println("####---------- After calling Scala:"+System.currentTimeMillis());
        System.out.println("####---------- Scala/outside, most word:"+mostWord._1+",count:"+mostWord._2+",total time:"+(System.currentTimeMillis()-start1));

        //start1 = System.currentTimeMillis();
        //SparkSession spark = SparkSession
        //.builder()
        //.appName("JavaWordCount")
        //.getOrCreate();
        //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD().cache();
        //val sortedWordCounts = file
        //    .flatMap(line=>line.split(" "))
        //    .map(word=>(word,1))
        //    .reduceByKey((a,b)=>a+b)
        //    .filter(_._1.length>0)
        //    .sortBy(_._2,false);

        //JavaPairRDD<String,Integer> wordCounts = lines
        //    .flatMap(line->Arrays.asList(line.split(" ")).iterator())
        //    .mapToPair(word -> new Tuple2<>(word, 1))
        //    .reduceByKey((a,b)->a+b)
        //    .filter(wordCnt->wordCnt._1().length()>0)
        //    .mapToPair(tuple->tuple.swap())
        //    .sortByKey(false,2)
        //    .mapToPair(tuple->tuple.swap());
        //wordCounts.foreach(tuple2->System.out.println("#### key:"+tuple2._1()+",value:"+tuple2._2()));
        //JavaRDD<String> words = lines.flatMap(
        //    new FlatMapFunction<String,String> () {
        //        public Iterator<String> call(String line) {
        //            return Arrays.asList(SPACE.split(line)).iterator();
        //////////////////        }
        //    } 
        //);
        //System.out.println("####---------- Java, Most count Word:"+wordCounts.first()._1()+",count:"+wordCounts.first()._2()+",net time:"+(System.currentTimeMillis()-start1));
    }
}


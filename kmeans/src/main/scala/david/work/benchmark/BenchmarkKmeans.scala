/*
 * Copyright (c) 2016.
 * David.Wang
 * Internal Use Only
 */

package david.work.benchmark
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wanghuaq on 8/26/2016.
  */
object BenchmarkKmeans {
    def main(args: Array[String]) {
        val usage = "BenchmarkKmeans <data file>"
        if(args.length!=1){
            println(usage)
            return
        }

        BenchSparkMllibKmeans(args(0))

    }

    def BenchSparkMllibKmeans(dataFileName:String):Unit={
        val conf = SparkConf().SetAppName("Spark Mllib Kmeans")
        val sc = SparkContext(conf)

        val test = new SparkMllibKmeans(sc, dataFileName)
        test.training()

    }
}

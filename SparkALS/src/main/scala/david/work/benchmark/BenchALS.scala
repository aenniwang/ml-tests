package david.work.benchmark

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by wanghuaq on 8/25/2016.
  */


object BenchALS {

    def main(args: Array[String]) {
        val usage = "BenchALS ratingDat, [movieDat]"

        if (args.length < 1) {
            println(usage)
            return
        }

        val ratingDat = args(0)
        val movieDat = {
            if (args.length > 1)
                args(1)
            else
                ""
        }
        println(s"BenchALS, rating file: $ratingDat, movie file $movieDat")

        //BenchSparkExplicitALS(ratingDat, movieDat)
        BenchDAALMPIALS(ratingDat,movieDat)
    }

    def BenchSparkExplicitALS(ratingDat:String, movieDat:String):Unit = {
        val sparkCf = new SparkConf().setAppName("ALS BenchMark")
        val sparkSc = new SparkContext(sparkCf)
        val alsTest = new ExplicitALSTrain(sparkSc,ratingDat,movieDat)
        alsTest.dataStatistic()
        alsTest.training()
    }
    def BenchDAALMPIALS(ratingDat:String, movieDat:String):Unit = {
        val sparkCf = new SparkConf().setAppName("ALS BenchMark")
        val sparkSc = new SparkContext(sparkCf)
        val alsTest = new DAALMPIALSTrain(sparkSc,ratingDat,movieDat)
        alsTest.dataStatistic()
        alsTest.training()
    }
}

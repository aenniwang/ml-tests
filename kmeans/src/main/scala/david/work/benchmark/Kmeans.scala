/*
 * Copyright (c) 2016.
 * David.Wang
 * Internal Use Only
 */

package david.work.benchmark

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.rdd.RDD

/**
  * Created by wanghuaq on 8/26/2016.
  */
class Kmeans(sc:SparkContext, dataFileName:String) extends Serializable {

     var numCluster = 3
     var numIterations = 20

    /**
      * What will happen if not caching it.
      * http://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd
      * Here, each branch issues a reload of the data. Adding an explicit cache statement will ensure
      * that processing done previously is preserved and reused.
      */
    private val traingData = sc.textFile(dataFileName).map(x=>Vectors.dense(x.split(' ').map(_.toDouble))).cache()

    def setClusterCount(clusterCount:Int):Int = {
        numCluster = clusterCount
        clusterCount
    }
    def getClusterCount = numCluster

    def setIterations (i:Int):Int = {
        numIterations = i
        numIterations
    }

    def getIterations = numIterations

    /**
      * sample statistic
      */
    def dataStatistic(): Unit = {
        val vectorCount = traingData.count()
        var vectorLenMin = Int.MaxValue
        var vectorLenMax = Int.MinValue

        traingData.foreach(x=>{
            if(x.size < vectorLenMin)
                vectorLenMin = x.size
            if(x.size > vectorLenMax)
                vectorLenMax =x.size
        })

        println("Kmeans sample data statistic")
        println(s"\t Vector Count: $vectorCount")
        println(s"\t Vector minimal size: $vectorLenMin")
        println(s"\t Vector maximum size: $vectorLenMax")
    }

    abstract def train(data:RDD[Vector]): Unit = {}

    def training(): Unit = {
        train(traingData)
    }
}

class SparkMllibKmeans(sc:SparkContext, fDataName:String) extends Kmeans(sc,fDataName){

    override def train(data:RDD[Vector]):Unit={
        val clusters = KMeans.train(data,numCluster,numIterations)
        println(s"Cluster Centers is ${clusters.clusterCenters}")
        val WSSSE = clusters.computeCost(data)
        println(s"Whithin Set Sum of Squared Errors = $WSSSE")
    }
}
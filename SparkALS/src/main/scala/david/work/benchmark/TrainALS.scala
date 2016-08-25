
package david.work.benchmark

import java.io.Serializable
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS=>mllibALS, Rating}

/**
  * Created by david on 9/17/14.
  */
/**
  *     Base class for Implicit/explicit ALS training
  *
  *     @param ratingFile: Rating file in Movie Len format: "UserID::ItemID::rating::timestamp"
  *     @param movieFile: Movie id and name pair
  */
class TrainALS(sc:SparkContext, ratingFile:String, movieFile:String) extends Serializable{
    /** read rating data from hdfs
      * output with RDD?
      * - for Spark, following processing will handle with RDDs
      * - for MPI, split source sample to the files that DAAL MPI required.
      */
    /**
      * Sample code: read data from hdfs without spark
      * Path pt=new Path("hdfs://npvm11.np.wc1.yellowpages.com:9000/user/john/abc.txt");
      * FileSystem fs = FileSystem.get(new Configuration());
      * BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
      */
    /**
      *Error:Caused by: java.io.NotSerializableException: org.apache.spark.SparkContext
      * "val ratingVals=sc.textFile(ratingFile).map(toRatings(_))", why????
      */
    val ratingVals = sc.textFile(ratingFile).map(l=>{
        val vals=l.split("::")
        (vals(0).toInt,vals(1).toInt,vals(2).toFloat)
    })

    def dataStatistic(): Unit = {
        val count = ratingVals.count()
        val users = ratingVals.map(l=>l._1).distinct()
        val items = ratingVals.map(l=>l._2).distinct()

        println("Dataset statistic:")
        println(s"\tTotal Sample count: $count")
        println(s"\tUser count:${users.count()}, min:${users.min()}, max:${users.max()}")
        println(s"\tItem count:${items.count()}, min:${items.min()}, max:${items.max()}")
    }

    def train(rVs:RDD[(Int,Int,Float)]): Unit = {
        println("In Class TrainALS")
    }

    def training():Unit = {
        print("Start training ... ")
        train(ratingVals)
        println("Finished")
    }

    private def toRatings(l:String):(Int, Int, Float)={
        val values=l.split("::")
        (values(0).toInt,values(1).toInt, values(2).toFloat)
    }
}

class ExplicitALSTrain(sc:SparkContext,ratingFile:String, movieFile:String) extends TrainALS(sc,ratingFile,movieFile){

    override def train(rVs:RDD[(Int,Int,Float)]):Unit ={
        println("Train In class ExplicitALSTrain")

        val iterator = 10
        val rank = 10
        val rating = rVs.map(l=>Rating(l._1,l._2,l._3))
        val model = mllibALS.train(rating, rank, iterator)

        model.save(sc,"/als_model")
    }
}

class ImplicitAlsTrain(sc:SparkContext,ratingFile:String, movieFile:String) extends TrainALS(sc,ratingFile,movieFile){

}

class DAALMPIALSTrain(sc:SparkContext,ratingFile:String, movieFile:String) extends TrainALS(sc,ratingFile,movieFile){
    var blockCount = 10

    def setBlockCount(nBlock:Int) = {
        blockCount = nBlock
    }

    def getBlockCount = blockCount

    def splitBlocks(): Unit ={

        // sort data, primary order is row, secondary order is column
      /** list sort
        * var l = List((1,2,0.1),(1,3,0.2),(23,2,0.5))
        * l.sortBy(x=>(x._1,x._2))
        *
        * And RDD works
        * scala> scz.sortBy(x=>(x._1,x._2)).collect
        * res4: Array[(Int, Double, Int)] = Array((1,0.1,1), (1,0.5,0), (2,0.1,2), (2,0.5,543), (3,0.4,32), (3,0.6,3), (4,0.3,63), (5,0.1,43), (9,0.2,0), (10,0.1,4), (100,0.1,6))
        */
      val rows = ratingVals.map(l=>l._1)
        val columns = ratingVals.map(l=>l._2)

        val rowMax=rows.max()
        val rowPerBlock=rowMax / blockCount

        for(i<-0 until blockCount){
            val tRowMin = i*rowPerBlock
            var tRowMax = tRowMin + rowPerBlock

            if (i eq (blockCount-1))
                tRowMax = rowMax


        }
    }
}

class DAALSparkALSTrain(sc:SparkContext,ratingFile:String, movieFile:String) extends TrainALS(sc,ratingFile,movieFile){

}

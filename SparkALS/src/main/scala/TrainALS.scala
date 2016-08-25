import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
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
class TrainALS(sc:SparkContext, ratingFile:String, movieFile:String) {
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

    val ratingVals=sc.textFile(ratingFile).map(toRatings(_))

    def DataStatistic(rVs:RDD[(Int,Int,Float)]): Unit = {
        val count = rVs.count()
        val users = rVs.map(l=>l._1).distinct()
        val items = rVs.map(l=>l._2).distinct()

        println("Dataset statistic:")
        println(s"\tTotal Sample count: $count")
        println(s"\tUser count:${users.count()}, min:${users.min()}, max:${users.max()}")
        println(s"\tItem count:${items.count()}, min:${items.min()}, max:${items.max()}")
    }

    def train(rVs:RDD[(Int,Int,Float)]): Unit = {
        println("In Class TrainALS")
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

}

class DAALSparkALSTrain extends TrainALS{

}
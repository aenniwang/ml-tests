/**
  * Created by david on 9/17/14.
  */
/**
  * Base class for Implicit/explicit ALS training
  */
class TrainALS(ratingFile:String, movieFile:String) {
  // rating format
  // "UserID::ItemID::rating::timestamp"
 def DataStatistic(): Unit ={

 }

  def train(): Unit ={

  }
}

class ExplicitALSTrain extends TrainALS{

}

class ImplicitAlsTrain extends TrainALS{

}

class DAALMPIALSTrain extends TrainALS{

}

class DAALSparkALSTrain extends TrainALS{

}
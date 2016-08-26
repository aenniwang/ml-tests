
### Prepare date ###
Data is downloaded from movie lens
upload movie lens data to hdfs
hadoop fs -put movies.dat /data/movielen/movies.dat
hadoop fs -put users.dat /data/movielen/users.dat


### sample data ###
Dataset statistic:
	Total Sample count: 1000209
        User count:6040, min:1, max:6040
        Item count:3706, min:1, max:3952

### split.py usage ###
- ** Install pandas **
pip install pandas


## DAAL ALS ##
Not a good algorithm, too complicated. Data need to be prehanled, but no guide line provided.
No way to predict the rating of a user for one movie.
Need ways to generate the model as a whole.
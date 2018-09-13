This lab was done by the combined effort of -

Redwan Ibne Seraj Khan ( UBITname :  rikhan  )
Alex Barganier         (UBITname : alexbarg )

At first put the folder DICLab3 in the home directory of the hadoop virtual machine.

For part 1, # The script follows the tutorial found at https://6chaoran.wordpress.com/2016/08/13/__trashed/

cd spark
./bin/spark-submit /home/hadoop/DICLab3/487Lab3/part1/src/titanic_spark.py


For part 2,
For completing the different tasks of the lab Spark and Python was used. We collected 1540 articles related to politics, sports, entertainment and business using NYtimes API. 
Afterwards we filtered the data that we collected into training and testing sets. The codes used for gathering and filtering data can be found inside 'gather_data' folder. The data set 
that we used can be found inside 'data' folder. After collecting the data we extracted the “words” or the “features” characterizing the category. We considered top 50 words to be the features. 
Afterwards we used Naive Bayes and Random Forest to train a model for our dataset. We used our model to predict articles in our test folders. We achieved an accuracy of approximately 60%. Then we 
collected articles from Washington Post and checked whether our model could correctly determine the articles in Washington Post. We saw our model showed the most accurate results while predictinf political articles. The details of various outputs and how to run the codes can be found inside 'videos' folder.


To run the code download the virtual image from https://buffalo.app.box.com/s/52did77hn2vjoje7iguf19btgs6vhvsc
Place the this submitted folder inside home.
Change the paths specified inside the different codes if necessary.

Then go to spark directory and enter the command given below to get the feature engineering output and machine learning model accuracy.

./bin/spark-submit /home/hadoop/DICLab3/487Lab3/spark/tf_idf.py 

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

##
## Script modified using the example of Spark script tf_idf_example.py
##
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import sys
from operator import add
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.mllib.classification import NaiveBayes 
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.evaluation import MulticlassMetrics

# Training paths
BUSINESS_TRAIN_PATH = '/home/hadoop/DICLab3/487Lab3/data/business_train/'
SPORTS_TRAIN_PATH = '/home/hadoop/DICLab3/487Lab3/data/sports_train/'
POLITICS_TRAIN_PATH = '/home/hadoop/DICLab3/487Lab3/data/politics_train/'
ENTERTAINMENT_TRAIN_PATH = '/home/hadoop/DICLab3/487Lab3/data/entertainment_train/'

# Testing paths
BUSINESS_TEST_PATH = '/home/hadoop/DICLab3/487Lab3/data/business_test/'
SPORTS_TEST_PATH = '/home/hadoop/DICLab3/487Lab3/data/sports_test/'
POLITICS_TEST_PATH = '/home/hadoop/DICLab3/487Lab3/data/politics_test/'
ENTERTAINMENT_TEST_PATH = '/home/hadoop/DICLab3/487Lab3/data/entertainment_test/'

# Final random test article paths
BUSINESS_ARTICLE_RANDOM = '/home/hadoop/DICLab3/487Lab3/data/random_test_articles/business_random.txt'
SPORTS_ARTICLE_RANDOM = '/home/hadoop/DICLab3/487Lab3/data/random_test_articles/sports_random.txt'
POLITICS_ARTICLE_RANDOM = '/home/hadoop/DICLab3/487Lab3/data/random_test_articles/politics_random.txt'
ENTERTAINMENT_ARTICLE_RANDOM = '/home/hadoop/DICLab3/487Lab3/data/random_test_articles/entertainment_random.txt'

# Labels
BUSINESS_LABEL = 0
SPORTS_LABEL = 1
POLITICS_LABEL = 2
ENTERTAINMENT_LABEL = 3

# Other const
APP_NAME = 'ARTICLE_CLASSIFIER_9000'

def buildTextRDD(directory, label_id):
	return sc.textFile(directory).map(lambda x: (label_id, x)).toDF()

def buildTfIdfRddAllTopics(business, sports, politics, entertainment):
	business_df = buildTextRDD(business, BUSINESS_LABEL)
	politics_df = buildTextRDD(sports, POLITICS_LABEL)
	sports_df = buildTextRDD(politics, SPORTS_LABEL)
	entertainment_df = buildTextRDD(entertainment, ENTERTAINMENT_LABEL)

	# Union together all dataframes
	main_df = business_df.union(politics_df)
	main_df = main_df.union(sports_df)
	main_df = main_df.union(entertainment_df)
	main_df = main_df.withColumnRenamed('_1', 'label')
	main_df = main_df.withColumnRenamed('_2', 'content')
	tokenizer = Tokenizer(inputCol="content", outputCol="words")
	wordsData = tokenizer.transform(main_df)
	hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=8)
	featurizedData = hashingTF.transform(wordsData)
	idf = IDF(inputCol="rawFeatures", outputCol="features")
	idfModel = idf.fit(featurizedData)
	rescaledData = idfModel.transform(featurizedData)
	return rescaledData.select([c for c in rescaledData.columns if c in ['label', 'features']]).rdd.map(lambda x: LabeledPoint(x.label, MLLibVectors.fromML(x.features)))

def runTestForModel(model, test_rdd):
	predictions = model.predict(test_rdd.map(lambda x: x.features))
	pred_labels = test_rdd.map(lambda x: x.label).zip(predictions)
	accuracy = 1.0 * pred_labels.filter(lambda pl: pl[0] != pl[1]).count() / test_rdd.count()
	return accuracy, pred_labels

if __name__ == "__main__":
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
	train_rdd = buildTfIdfRddAllTopics(BUSINESS_TRAIN_PATH, SPORTS_TRAIN_PATH, POLITICS_TRAIN_PATH, ENTERTAINMENT_TRAIN_PATH)
	test_rdd = buildTfIdfRddAllTopics(BUSINESS_TEST_PATH, SPORTS_TEST_PATH, POLITICS_TEST_PATH, ENTERTAINMENT_TEST_PATH)
	f = open("/home/hadoop/DICLab3/featureengineeringtrainrdd.txt","w+")
	f.write(str(train_rdd.take(5000)))
	f.seek(0)
	f.close()
	f = open("/home/hadoop/DICLab3/featureengineeringtestrdd.txt","w+")
	f.write(str(test_rdd.take(5000)))
	f.close()

	# Train Random Forest model & get accuracy/confusion matrix
	rf_model = RandomForest.trainClassifier(train_rdd, numClasses=4, categoricalFeaturesInfo={}, numTrees=8)
	rf_accuracy, rf_pred_labels = runTestForModel(rf_model, test_rdd)

	# Train Naive Bayes model & get accuracy/confusion matrix
	nb_model = NaiveBayes.train(train_rdd, 2.0)
	nb_accuracy, nb_pred_labels = runTestForModel(nb_model, test_rdd)
	
	
	# Print evaluation metrics
	
	f = open("/home/hadoop/DICLab3/EvaluationMetrics.txt","w+")
	f.write('NaiveBayes model accuracy {}\nRandomForest model accuracy {}\nClass label IDs: Business = {}, Sports = {}, Politics = {}, Entertainment = {}'.format(nb_accuracy,rf_accuracy,BUSINESS_LABEL,SPORTS_LABEL, POLITICS_LABEL, ENTERTAINMENT_LABEL))
	f.close();
	print('NaiveBayes model accuracy {}'.format(nb_accuracy))
	print('RandomForest model accuracy {}'.format(rf_accuracy))
	print('Class label IDs: Business = {}, Sports = {}, Politics = {}, Entertainment = {}'.format(BUSINESS_LABEL, SPORTS_LABEL, POLITICS_LABEL, ENTERTAINMENT_LABEL))
	# Part 5: Testing with random articles
	random_test_rdd = buildTfIdfRddAllTopics(BUSINESS_ARTICLE_RANDOM, SPORTS_ARTICLE_RANDOM, POLITICS_ARTICLE_RANDOM, ENTERTAINMENT_ARTICLE_RANDOM)

	rf_random_accuracy, rf_random_labels = runTestForModel(rf_model, random_test_rdd)
	nb_random_accuracy, nb_random_labels = runTestForModel(nb_model, random_test_rdd)
	f = open("/home/hadoop/DICLab3/PredictionWashington.txt","w+")
	f.write('Part 5 Prediction Labels (format is [(actual_label, predicted_label),...]):\nRandomForest:\n\t{}\nNaiveBayes:\n\t{}'.format(str(rf_random_labels.collect()), str(nb_random_labels.collect())))
	f.close()
	
	f = open("/home/hadoop/DICLab3/PredictionNYtimes.txt","w+")
	f.write('Train vs. Test for NYtimes data (format is [(actual_label, predicted_label),...]):\nRandomForest:\n\t{}\nNaiveBayes:\n\t{}'.format(str(rf_pred_labels.collect()), str(nb_pred_labels.collect())))
	f.close()

	print('Train vs. Test for NYtimes data (format is [(actual_label, predicted_label),...]):\nRandomForest:\n\t{}\nNaiveBayes:\n\t{}'.format(str(rf_pred_labels.collect()), str(nb_pred_labels.collect())))
	print('Part 5 Prediction Labels (format is [(actual_label, predicted_label),...]):\nRandomForest:\n\t{}\nNaiveBayes:\n\t{}'.format(str(rf_random_labels.collect()), str(nb_random_labels.collect())))
	spark.stop()

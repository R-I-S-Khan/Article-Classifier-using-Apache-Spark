import os.path
import csv
import math
import re
import nltk
from nltk.corpus import stopwords

filter_keywords = [
	{
		'filename_prefix': 'politics',
		'train_dir': '../data/politics_train/',
		'test_dir': '../data/politics_test/',
		'source_file': '../data/politics.csv',
		'url_keywords': ['/us/politics/']
	},
	{
		'filename_prefix': 'business',
		'train_dir': '../data/business_train/',
		'test_dir': '../data/business_test/',
		'source_file': '../data/business.csv',
		'url_keywords': ['/business/economy/', '/business/global/', '/business/']
	},
	{
		'filename_prefix': 'sports',
		'train_dir': '../data/sports_train/',
		'test_dir': '../data/sports_test/',
		'source_file': '../data/sports.csv',
		'url_keywords': ['/sports/']
	},
	{
		'filename_prefix': 'entertainment',
		'train_dir': '../data/entertainment_train/',
		'test_dir': '../data/entertainment_test/',
		'source_file': '../data/entertainment.csv',
		'url_keywords': ['/arts/television/', '/arts/music/', '/arts/movies/', '/movies/']
	}
]
stop_words = set(stopwords.words('english'))

def write_to_file(fn_prefix, dict):
	i = 1
	for url, content in dict.items():
		fh = open(fn_prefix+'_'+str(i)+'.txt', 'w')
		fh.write(' '.join([word for word in re.sub(r'[^\w\s]',' ',content).rstrip().split() if word not in stop_words])+'\n')
		fh.close()
		i = i + 1

for topic in filter_keywords:
	read_dict = None
	source_file = topic['source_file']
	with open(source_file, 'r') as csv_file:
		reader = csv.reader(csv_file)
		read_dict = dict(reader)
		file_exists = True
	filtered_dict = { url:article for (url,article) in read_dict.items() if any(substring in url for substring in topic['url_keywords']) }
	split_index = math.floor((len(filtered_dict)/4)*3)
	test_dict = dict(list(filtered_dict.items())[split_index:])
	train_dict = dict(list(filtered_dict.items())[:split_index])
	train_filename_pref = topic['train_dir']+topic['filename_prefix']+'_train'
	test_filename_pref = topic['test_dir']+topic['filename_prefix']+'_test'
	# write training data to file
	write_to_file(train_filename_pref, train_dict)
	# write test data to file	
	write_to_file(test_filename_pref, test_dict)
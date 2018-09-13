# Alex Barganier - alexbarg@buffalo.edu
import requests
import json
from bs4 import BeautifulSoup
import os.path
import time
import csv
from datetime import timedelta, date

debug = False
topics = [
	{	
		# Sports
		'query_terms': 'football%2Bsoccer%2Bball%2Bbaseball%2Bgolf%2Btennis%2Bhockey%2BNHL%2Bteam%2BFIFA%2Bolympic%2BEagles%2BBills%2BDolphins%2BPatriots%2BJets%2BRavens%2BBengals%2BSteelers%2BTexans%2BColts%2BJaguars%2BTitansaseball%2BBroncos%2BCowboys%2BGiants%2BRedskins%2BBears%2BLions%2Bathlete%2Bmedal%2Bplayoff%2Bstrike%2Byankees%2BNFL%2BNBA',
		'filename': '../data/sports.csv'
	},
	{	
		# Business
		'query_terms': 'supply%2Bdemand%2Bstock%2Bmarket%2Bbond%2Bbank%2Bsavings%2Binvestment%2Bprofit%2Bloss%2Blawsuit%2Btrade%2B$%2Beconomy%2Bpolicymaker%2Bclient%2Bexport%2Bimport%2Bbillion%2Bmillion%2Bbudget%2BCEO%2Bbill%2Bstores%2BTax%2BIRS%2BCredit%2BDebit%2Bcapital%2Bfirm%2BFederal%2Breserve%2Btariff',
		'filename': '../data/business.csv'
	},
	{	
		# Politics
		'query_terms': 'congress%2Bdemocracy%2Bdemocratic%2Belection%2Bpresident%2Bsenate%2Bsenator%2Bcongressman%2Bcongresswoman%2Bcongressmen%2Bcongresswomen%2Bcampaign%2Bdelegate%2Btrump%2Bschumer%2Btillerson',
		'filename': '../data/politics.csv'
	},
	{
		# Entertainment
		'query_terms': 'movie%2Bseries%2Bnetflix%2BHulu%2BHBO%2Bfilm%2Bdirector%2Bproducer%2Brelease%2Bseason%2Brap%2Borchestra%2Bviolin%2BJazz%2Balbum%2Bpop%2Brock%2Bhip-hop%2Btelevision%2BTV%2Boscars%2Bgrammys',
		'filename': '../data/entertainment.csv'
	}
]

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def get_article_body(url):
	session = requests.Session()
	req = session.get(url)
	soup = BeautifulSoup(req.text, 'html.parser')
	paragraphs = soup.find_all('p', class_='story-body-text story-content')

	article = ''
	for i in range(len(paragraphs)):
		article = article + paragraphs[i].get_text()
	return article
start_date = date(2013, 1, 1)
end_date = date(2018, 4, 23)
dates = daterange(start_date, end_date)

# Sleeps for 5 seconds between each iteration. This is due to API limits.
for single_date in daterange(start_date, end_date):	
	for topic in topics:
		date = str(single_date)
		filename = topic['filename']
		read_dict = {}
		file_exists = False
		if os.path.isfile(filename): 
			with open(filename, 'r') as csv_file:
				reader = csv.reader(csv_file)
				read_dict = dict(reader)
				file_exists = True

		query_terms = topic['query_terms']
		api_key = '1d7f48d3e65a4e9480a29c3cdd2e84c7'
		query_string = 'api-key='+api_key+'&q='+query_terms+'&begin_date='+date+'&end_date='+date
		base_url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?'
		print('Performing API request for date ' + date + ' with URL: ' + base_url+query_string+ '\n')
		r = requests.get(base_url+query_string)
		data = r.json()
		articles = {}
		print('Analyzing API results...')
		for response in data['response'].items():
			if debug:
				print(response)
			for result in response[1]:
				if type(result) is dict:
					url = result['web_url']	
					# if we already have the file, check for uniqueness of URL before doing work to parse
					if file_exists: 
						if url not in read_dict:
							article = get_article_body(url)
							if len(article) > 0:
								print('The article at the URL ' + url + ' has been marked to be saved to ' + filename)	
								articles[url] = article
							else:
								print('[WARN]: The URL ' + url + ' either returned no data or was not able to be parsed correctly.')
						else:
							print('The article at the URL ' + url + ' already exists in ' + filename)	
					# If the file doesn't exist, all URLs are unique
					else:
						article = get_article_body(url)
						if len(article) > 0:
							print('The article at the URL ' + url + ' has been marked to be saved to ' + filename)	
							articles[url] = article
						else:
							print('[WARN]: The URL ' + url + ' either returned no data or was not able to be parsed correctly.')

		sum = 0
		with open(filename, 'a') as csv_file:
				writer = csv.writer(csv_file)
				for key, value in articles.items():
					if file_exists: 
						if key not in read_dict:
							sum = sum + 1
							writer.writerow([key, value])	
					else:
						sum = sum + 1
						writer.writerow([key, value])
		print("Added " + str(sum) + " new unique articles to " + filename + '\n')
		time.sleep(2)
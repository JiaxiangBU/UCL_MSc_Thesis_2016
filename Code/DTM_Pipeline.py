#!/usr/bin/env python
'''
Author: Chris Martin
Date: 2016-07-11
Purpose: Script for running DTM pipeline
'''


# ===================================================
# 0.0 - Imports
# ===================================================

from __future__ import print_function
import logging
import os
import nltk
import gensim
from gensim.corpora.dictionary import Dictionary
from gensim import corpora
from gensim.models.wrappers.dtmmodel import DtmModel
from gensim import corpora, utils
import os
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
import os
import wordcloud
from wordcloud import WordCloud
import pandas as pd
import re
import time
import itertools
import copy
import sys
import pickle
import random

import nltk
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet

from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

_MODELS_DIR = "saved_models/"


# ===================================================
# 1.0 - Functions
# ===================================================

def progress(percent):
    '''Just a progress bar to be printed to the screen
    '''
    width = 10
    perc = np.floor(percent*width)
    prog = "="*perc + ">"
    if percent != 1:
        print('\r' + 'Progress: [{}] {}%'.format(prog.ljust(width+1), str(percent*100)),end="")
    else:
        print('\r' + 'Progress: [{}] {}%'.format(prog.ljust(width+1), str(percent*100)),end="")
    sys.stdout.flush()

def stream_corp(path,approved_ids=None):
    '''generator for iterating through lines of the data
    '''
    # regex for sanitizing the abstracts
    html = re.compile(r'\<[^\>]*\>')
    nonan = re.compile(r'[^a-zA-Z ]')

      # read file one line at a time and sanitize
    for line in pd.read_csv(path,sep=',', chunksize=1):
        if approved_ids == None:
            line = line["appln_abstract"].values[0]
            line = nonan.sub(' ',html.sub('',str(line))).lower().split()
            line = stem_doc(line) #apply lemmatization/stemming
            yield line
        else: # if there are approved_ids
            if line["appln_id"].values[0] in approved_ids:
                line = line["appln_abstract"].values[0]
                line = nonan.sub(' ',html.sub('',str(line))).lower().split()
                line = stem_doc(line) #apply lemmatization/stemming
                yield line

def get_time_seq(data_file, min_slice_size=None):
	df =  pd.read_csv(data_file)
	# Create dummy column
	df["Y"] = pd.DatetimeIndex(df["appln_filing_date"]).to_period("A")
	# group by dummy column
	groups = df.groupby("Y")
	# return sorted df and counts dict
	#df = df.sort_values("appln_filing_date")
	approved_ids = None
	if min_slice_size == None:
		# count members of each group
		counts = np.sort([[key,len(groups.groups[key])] for key in groups.groups.keys()], axis=0)
		time_seq = list(counts[:,1])
	else:
		approved_ids = []
		for group in groups.groups.iteritems():
			if len(group[1]) >= min_slice_size:
				approved_ids.append(df.loc[group[1]]["appln_id"].values[:min_slice_size])
		time_seq = [min_slice_size]*len(approved_ids)
	return time_seq, approved_ids

def get_wordnet_pos(treebank_tag):
	tag_to_type = {'J': wordnet.ADJ, 'V': wordnet.VERB, 'R': wordnet.ADV}
	return tag_to_type.get(treebank_tag[:1], wordnet.NOUN)

def stem_doc(doc):
	lmtzr = WordNetLemmatizer()
	tags = nltk.pos_tag(doc)
	return [lmtzr.lemmatize(word, get_wordnet_pos(tag[1])) for word, tag in zip(doc, tags)]

# ===================================================
# 2.0 - Corpus Creation
# ===================================================

class MyCorpus(object):
    def __init__(self, data_file,approved_ids=None):
        self.data_file = data_file
        self.approved_ids = approved_ids

    def __iter__(self):
        # regex for sanitizing the abstracts
        html = re.compile(r'\<[^\>]*\>')
        nonan = re.compile(r'[^a-zA-Z ]')
        
        # read file one line at a time and sanitize
        for line in pd.read_csv(self.data_file,sep=',', chunksize=1):
            if self.approved_ids == None:
                line = line["appln_abstract"].values[0]
                line = nonan.sub(' ',html.sub('',str(line))).lower().split()
                line = stem_doc(line) #apply lemmatization/stemming
                yield self.dictionary.doc2bow(line)
            else: # if there are approved_ids
                if line["appln_id"].values[0] in self.approved_ids:
                    line = line["appln_abstract"].values[0]
                    line = nonan.sub(' ',html.sub('',str(line))).lower().split()
                    line = stem_doc(line) #apply lemmatization/stemming
                    yield self.dictionary.doc2bow(line)

    def make_dictionary(self, stoplist=False, minfreq = 1):
        # create dictionary
        self.dictionary = corpora.Dictionary(line for line in stream_corp(self.data_file))

        if stoplist != False:
            # ids to remove stop words
            stop_ids = [self.dictionary.token2id[stopword] for stopword in stoplist
                        if stopword in self.dictionary.token2id]
        else:
            stop_ids = []
            
        # ids to remove words that appear 'minfreq' or fewer times.
        once_ids = [tokenid for tokenid, docfreq in self.dictionary.dfs.iteritems() if docfreq <= minfreq]
        
        # remove stop words and words that appear only once
        self.dictionary.filter_tokens(stop_ids + once_ids) 
        
        # remove gaps in id sequence after words that were removed
        self.dictionary.compactify() 

    def get_texts(self):
        self.docs = [doc for doc in self]
        return self.docs

    def __len__(self):
        return len(self.docs)


# ===================================================
# 3.0 - LDA Pipeline
# ===================================================

class Pipeline(object):
	def __init__(self, key="Y02E_10", m_type="LDA", num_topics=7, min_slice_size=200):
		self.num_topics = num_topics
		self.key = key
		self.data_file = '../Data/{}.csv'.format(key)
		self.dict_file = "{}.dict".format(key)
		self.corpus_file = "{}.mm".format(key)
		self.coords_file = "{}_coords.csv".format(key)
		self.topics_file = "{}_Topics.txt".format(key)
		self.approved_ids_file = "{}_approved_ids".format(key)
		self.m_type = m_type
		self.min_slice_size = min_slice_size

		dtm_home = os.environ.get('DTM_HOME', "dtm-master")
		self.dtm_path = os.path.join(dtm_home, 'bin', 'dtm-darwin64') if dtm_home else None

	def make_corpus(self):
		# stop list from nltk
		stoplist = set(nltk.corpus.stopwords.words("english"))

		if self.m_type in ["DTM","DIM"]:
			# time shape stuff
			self.time_seq, self.approved_ids = get_time_seq(self.data_file, self.min_slice_size)

			filehandler = open(_MODELS_DIR + self.approved_ids_file + ".obj","wb")
			pickle.dump(self.approved_ids,filehandler)
			filehandler.close()

			self.approved_ids = list(itertools.chain(*self.approved_ids))
			#self.corpus = DTMcorpus(self.corpus) # warning, this reads in the whole corpus to memory!
			self.corpus = MyCorpus(self.data_file,self.approved_ids)
		else:
			# instantiate corpus object
			self.corpus = MyCorpus(self.data_file) # memory friendly corpus!

		print("Making dictionary")
		t0 = time.time()
		# create dictionary, remove stopwords and words only occurring once, apply stemming
		self.corpus.make_dictionary(stoplist=stoplist, minfreq=25) # minfreq=25 to match Blei's paper
		print(time.time() - t0)

		print("saving dictionary")
		t0 = time.time()
		# save the dictionary
		self.corpus.dictionary.save(_MODELS_DIR + self.dict_file)
		print(time.time() - t0)

		print("Saving corpus")
		t0 = time.time()
		# save the corpus
		gensim.corpora.MmCorpus.serialize(_MODELS_DIR + self.corpus_file, self.corpus)
		print(time.time() - t0)

	def run_model(self):
		''' Run the LDA model on a given corpus and dictionary
		'''
		if not hasattr(self, 'corpus'):
			# if there's no corpus present, read in saved corpus
			corpus = gensim.corpora.MmCorpus(os.path.join(_MODELS_DIR, self.corpus_file))

		if not hasattr(self, 'corpus.dictionary'):
			# if there's no dictionary present, read in saved dictionary
			dictionary = gensim.corpora.Dictionary.load(os.path.join(_MODELS_DIR, 
			                                            self.dict_file))
		if self.m_type == "LDA":
			# Run LDA model
			print("Running LDA Model")
			t0 = time.time()
			self.lda = gensim.models.LdaModel(self.corpus, id2word=self.corpus.dictionary,num_topics=self.num_topics)
			print(time.time() - t0)
		if self.m_type == "DTM":
			print("Running DTM Model")
			t0 = time.time()
			self.lda = DtmModel(self.dtm_path,self.corpus,self.time_seq,num_topics=self.num_topics,id2word=self.corpus.dictionary,initialize_lda=True)
			print(time.time() - t0)
		if self.m_type == "DIM":
			print("Running DIM Model")
			t0 = time.time()
			self.lda = DtmModel(self.dtm_path,self.corpus,self.time_seq,num_topics=self.num_topics,model="fixed",id2word=self.corpus.dictionary,initialize_lda=True)
			print(time.time() - t0)

	def save_model(self, model_name="LDA_model"):
		''' Save the current LDA model to an object file in the saved models folder.
		'''
		filehandler = open(_MODELS_DIR + model_name + ".obj","wb")
		pickle.dump(self.lda,filehandler)
		filehandler.close()

	def topics(self,model_name=None,save=True,viz=True):
		'''Will print and optionally save topics of a given LDA model. Uses LDA model present in object by default
		unless alternate saved version is specified. 
		'''
		# If model != None:
		# 		try to get it from the folder
		#		except, it's not there, throw an error.
	
		if model_name != None:
			filehandler = open(_MODELS_DIR + model_name + ".obj",'r')
			self.lda = pickle.load(filehandler)
			filehandler.close()

		if not hasattr(self, 'lda'):
			print("no LDA model detected")
		
		else:
			print(self.lda.print_topics(self.num_topics, num_words=25))
			self.topics = self.lda.show_topics(num_topics=self.num_topics, num_words=20)
			
			# save the topics and their constituent words
			if save:
				f = open(_MODELS_DIR+self.topics_file, 'w')
				print(self.topics, end="", file=f)
				f.close()
			
			if viz:
				if not hasattr(self,"topics"):
					self.topics = open(os.path.join(_MODELS_DIR, self.topics_file), 'rb').readlines()[0]
					self.topics.close()
				# parsing topic string
				lines = str(self.topics).strip("[()").strip("]\n").split("(")
				lines = [i.strip("),").split(", u'")[1] for i in lines]
				
				# plotting word clouds of each topic
				curr_topic = 0
				#classes = np.array(target_labels)[np.array(list(manual_best)) - 1]
				for j, line in enumerate(lines):
					scores = [float(x.split("*")[0]) for x in line.split(" + ")]
					words = [x.split("*")[1].strip("'), ") for x in line.split(" + ")]

					freqs = []
					for word, score in zip(words, scores):
						freqs.append((word, score))

					wc = WordCloud(max_words=100)
					elements = wc.fit_words(freqs)
					default_colors = wc.to_array()
					plt.figure()
					plt.title("Topic {}".format(j))#classes[j])
					plt.imshow(default_colors)
					plt.axis("off")
					plt.show()
					curr_topic += 1
				
def main():
	# from DTM_Pipeline import Pipeline
	pl = Pipeline(key="Y02E_10_20",m_type="DIM",num_topics=10)
	pl.make_corpus()
	pl.run_model()
	#pl.topics()
	pl.save_model(model_name="DIM_model")

# ===================================================
# etc.
# ===================================================
if __name__ == '__main__':
	main()
	
























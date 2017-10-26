from __future__ import print_function
import threading
from Stemmer import Stemmer 
from collections import *
import sys
import math
import re
from sets import Set
import operator
reload(sys)
args=sys.argv
stemmer = Stemmer('english')


def textProcess(word):
	stemword = ''
	if word :
		if word not in stopWords:
			word = word.lower()
			stemword = stemmer.stemWord(word)
	return stemword

class searchParallel(threading.Thread):
	
	def __init__(self, idno, term,nDoc):
		threading.Thread.__init__(self)
		self.idno = idno
		self.term = term
		self.nDoc = nDoc

	def findFile(self,query,l,r):
		if(l==r):
			return l
		mid = (l+r)/2 
		if(query<=offset[mid]):
			return self.findFile(query,l,mid)
		return self.findFile(query,mid+1,r)

	def extCnt(self,docinfo,x,ind,docId,idf):
		# print(docinfo)
		tmp = docinfo.split(x)
		if(len(tmp)>1):
	   		Cnt = int(tmp[0])
	   		tfidf = float(math.log10(Cnt*10+1))*idf
	   		docDict[self.idno][docId][ind]=tfidf
	   		return tmp[1]
	   	return tmp[0]

	def run(self):
		query = textProcess(self.term)
		if query not in docFreq :
			return
		idf = math.log10(1+(float(self.nDoc)/float(docFreq[query])))
		# print(query,":",idf)
		# print(docFreq[query])
		# print(query)
		if query : 
			fileNo = self.findFile(query,0,int(args[1]))
		   	# print(fileNo)	
	   	with open('./index/new'+str(fileNo),'r') as f:
	   		for line in f :
	   			# print(line)
	   			line = line.strip()
	   			word = line.split(':')[0]
	   			wIndex = line.split(':')[1]
	   			# print(word)
	   			if(word==query):
	   				docs = wIndex.split("|")[0:-1]
	   				# print(docs)
	   				for i in docs : 
	   					docId = i.split('d')[0]
	   					(docDict[self.idno])[docId]=[0,0,0,0,0] #[title,text,category,info,external]
	   					i = i.split('d')[1]
	   					# print(i)
	   					i = self.extCnt(i,'t',0,docId,idf)
	   					i = self.extCnt(i,'p',1,docId,idf)
	   					i = self.extCnt(i,'c',2,docId,idf)
	   					i = self.extCnt(i,'i',3,docId,idf)
	   					i = self.extCnt(i,'e',4,docId,idf)
	   				break

offset = []
title = {}
docFreq = {}
docSize = {}
stopWords = defaultdict(bool)

f1 = open('stopwords','r')  
lines = f1.readlines()
for line in lines:
    if line :
        line = line.strip('\n')
        stemStop = stemmer.stemWord(line)
        stopWords[line] = True
        stopWords[stemStop] = True
f1.close()

with open('./index/offset.txt','r') as f:
	for line in f:
		offset.append(line.strip())

with open('./index/title.txt','r') as f:
	for line in f:
		line = line.strip()
		words = line.split(':')
		docId = int(words[0])
		tmp = words[1]
		title[docId]=tmp

with open('./index/docSize.txt','r') as f:
	for line in f:
		line = line.strip()
		words = line.split(':')
		docId = int(words[0])
		tmp = words[1]
		docSize[docId]=tmp

with open('./index/termFreq.txt','r') as f:
	for line in f:
		line = line.strip()
		words = line.split(':')
		docId = words[0]
		tmp = int(words[1])
		docFreq[docId]=tmp

# queryTerms = input("Enter your query : ")
while(1):
	fieldType = '1' 
	findInd={'1':0,'2':2,'3':3}
	qtype = raw_input("Type 1 for normal query and 2 for field query : ")
	if(qtype=='2'):
		fieldType = raw_input("Enter field type number : 1.Title 2.Category 3. Infobox : ")
	fieldInd = findInd[fieldType]
	# print(fieldInd)
	inputQuery = raw_input("Enter you query : ")
	# print(inputQuery)
	queryTerms = re.split("[^A-Za-z0-9]",inputQuery)
	# print(queryTerms)

	for j in queryTerms : 
		if not j :
			queryTerms.remove(j)
	# print(queryTerms)
	nq = len(queryTerms)

	docDict = [{} for i in xrange(nq)]

	threads = [0 for i in xrange(20)]
	i = 0
	if(nq > 18):
		print("too long query")

	else : 
		lo = 0
		for i in xrange(nq):
			if(i>=4):
				# print("hey")
				threads[i-4].join()
				lo=i-4+1
			threads[i]=searchParallel(i,queryTerms[i],5000)
			threads[i].start()

		for i in xrange(lo,nq):
			threads[i].join()

	docVocab = []
	for i in xrange(nq):
		docVocab.extend(docDict[i].keys())
	docVocab = Set(docVocab)
	# print(docVocab)

	score = defaultdict(float)
	for doc in docVocab :
		for i in xrange(nq):
			querycnt = 0
			titleWeight = 10
			if doc in docDict[i] :
				tmp1 = math.log10(float(docSize[int(doc)]))
				score[doc] += 1
				# querycnt += 1
				# titleWeight += 10*querycnt*querycnt
				# print(doc)
				# print(docSize[int(doc)])
				tmp = docDict[i][doc]
				# print(doc,tmp)
				if(int(qtype)==1):
					if(tmp[0]!=0):
						titleWeight=titleWeight**3
					score[doc] += 2*tmp[0]+float(4*tmp[2]+tmp[3]+tmp[1])/float(tmp1)
				else :
					score[doc] += tmp[fieldInd]
				# print(docSize[int(doc)])
				# print(tmp1)
				# score[doc] = float(score[doc])/tmp1 

	# print(score[967])
	sorted_score = sorted(score.items(), key=operator.itemgetter(1), reverse=True)

	for i in sorted_score[0:10] :
		print(i[0],i[1],title[int(i[0])])
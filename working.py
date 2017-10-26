from __future__ import print_function
import xml.etree.ElementTree as etree
import codecs
import csv
import bz2
import heapq
import time
import os,re
from collections import *
import sys
from Stemmer import Stemmer 
reload(sys)
args=sys.argv
sys.setdefaultencoding('utf-8')
stemmer = Stemmer('english')

pathWikiXML = args[1]
docId,fileCount = 0,0
types = 5
extracttype = {'title':0, 'text': 1,'category': 2,'infobox' : 3,'externLinks' : 4}
delim = {0 :'t', 1 : 'p', 2 : 'c', 3 : 'i' , 4 : 'e'}

cnt = [defaultdict(int) for i in range(types)]
invindex = defaultdict(list)
stopWords = defaultdict(bool)
punc = defaultdict(bool)
docFreq = defaultdict(int)
docSize = defaultdict(int)
totalwords = {}
titleList = []

f1 = open('stopwords','r')  
lines = f1.readlines()
for line in lines:
    if line :
        line = line.strip('\n')
        stemStop = stemmer.stemWord(line)
        stopWords[line] = True
        stopWords[stemStop] = True
f1.close()
f1 = open('punc','r')   
lines = f1.readlines()
for line in lines:
    line = line.strip('\n')
    punc[line] = True
f1.close()

def strip_tag_name(t):
    # t = elem.tag
    idx = k = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

def tokenize(txt):
    return re.split("[^A-Za-z]", txt)


def textProcess(txt):
    tmp = list(txt)
    if(len(tmp)>0):
        if tmp[-1] in punc :
            tmp.pop()
        if tmp[0] in punc :
            tmp.pop(0)
    st = ''.join(tmp)
    return st

def insertDict(word,ind):
    if word :
        if word not in stopWords:
            word = word.lower()
            stemword = stemmer.stemWord(word)
            if stemword not in stopWords:
                if stemword :
                    cnt[ind][stemword] += 1
                    totalwords[stemword] = 1

def extractText(data,docId):
    lines = data.split('\n')
    nlines = len(lines)
    tofind =['{{infobox', '{{ infobox' , '{{Infobox' , '{{ Infobox']
    for i in xrange(nlines):

        if '[[Category:' in lines[i] :
            lines[i] = lines[i].strip('[')
            lines[i] = lines[i].strip(']')
            # print(lines[i])
            words = re.split('[:;#@,|()"* ]',lines[i])[1:]
            for word in words : 
                insertDict(word,2)

        elif '{{infobox' in lines[i] or '{{ infobox' in lines[i] or '{{Infobox' in lines[i] or '{{ Infobox' in lines[i]:
            # print(lines[i])
            flag=lines[i].count('{{')-lines[i].count('}}')
            # print(flag)
            temp = re.split('[^A-Za-z]',lines[i])[1:]
            for word in temp :
                insertDict(word,3)
            i+=1
            # tmpCnt = 0
            while flag>0 and i<nlines-1 :
                # print(lines[i])
                if '{{' in lines[i]:
                    count=lines[i].count('{{')
                    flag+=count
                if '}}' in lines[i]:
                    count=lines[i].count('}}')
                    flag-=count
                words = re.split('[^A-Za-z]',lines[i])
                for word in words :
                    insertDict(word,3)
                i+=1
            # print("ends")
            i-=1

        elif '==external links==' in lines[i] or'== external links ==' in lines[i] or'==External Links==' in lines[i] or'== External links ==' in lines[i] :
            # print(lines[i]) 
            tmpCnt = 0
            while('* [' not in lines[i] and '*[' not in lines[i] and i<nlines-1 and tmpCnt<30):
                # print(lines[i])
                i+=1
                tmpCnt+=1
            while('* [' in lines[i] or '*[' in lines[i]):
                lines[i] = lines[i].replace('/','')
                # print(lines[i])
                words = re.split(' ',lines[i])[2:]
                for word in words :
                    subwords =  re.split("[^A-Za-z]",word)
                    # print(subwords)
                    for subw in subwords :
                        insertDict(subw,4) 
                i+=1
                if(i>=nlines):
                    break
            i-=1

        else :
            words = re.split("[^A-Za-z]",lines[i])
            docSize[docId]+=len(words)
            for word in words :
                insertDict(word,1)



def writeFile(filePath,data,cnt):
    # filename = './index/'+str(args[2])+str(cnt)+'.txt.bz2'
    # with bz2.BZ2File(filename, 'wb', compresslevel=9) as f:
    filename = './index/'+str(args[2])+str(cnt)
    with open(filename,'wb') as f : 
        for word in sorted(data):
            tmpstr = word + ':' 
            for tmp in data[word]:
                tmpstr += tmp + '|'
            print(tmpstr, file=f)
        # print(len(invindex))
    f.close()
        
def stripCamel(txt):
    splitted = re.sub('(?!^)([A-Z][a-z]+)', r' \1', txt).split()
    return splitted

for event, elem in etree.iterparse(pathWikiXML, events=('start', 'end')):
    # print(elem.tag)
    tname = strip_tag_name(elem.tag)
    # print(tname)

    
    if tname == 'page' and event == 'end':
        if(docSize[docId] > 50 ):
            for word in totalwords:
                tmpstr = str(docId) + 'd'
                for j in range(types):
                    if cnt[j][word] != 0:
                        tmpstr += str(cnt[j][word]) + delim[j] 
                invindex[word].append(tmpstr)
                docFreq[word]+=1
        totalwords.clear()
        for i in xrange(types):
            cnt[i].clear()
        docId += 1
        if(docId%20000==0):
            print(docId)
            writeFile(args[2],invindex,fileCount)
            fileCount+=1
            invindex.clear()
        
    if tname =='text' and event == 'end':
        # print(elem.text)
        words = extractText(str(elem.text),docId)

    if tname=='title' and event == 'end':
        titleList.append(str(elem.text))
        splitted = stripCamel(str(elem.text))
        # print(splitted)
        for indi in splitted :
            words = re.split('[:,|()/"* ]',str(indi))
            for word in words:
                insertDict(word,0)
    
f1 = open('./index/title.txt','wb')
for i,title in enumerate(titleList) :
    print(str(i)+":"+title , file=f1)
f1.close()

f1 = open('./index/termFreq.txt','wb')
for term in sorted(docFreq) :
    print(term+":"+str(docFreq[term]) , file=f1)
f1.close()

f1 = open('./index/docSize.txt','wb')
for i in xrange(docId) :
    print(str(i)+":"+str(docSize[i]) , file=f1)
f1.close()
# mergeFiles()

writeFile(args[2],invindex,fileCount)
print(fileCount)


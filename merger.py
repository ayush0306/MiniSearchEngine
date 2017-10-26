from __future__ import print_function
import heapq as pq
import os
import sys
reload(sys)
args=sys.argv

def mergeFiles(nFiles):
    wordHeap = []
    idHeap = []
    lastWord = []
    finish = [0 for i in xrange(nFiles)]
    currLine = {}
    sep = {}
    filePointer = {}
    for i in xrange(nFiles):
        filename = './index/out'+str(i)
        filePointer[i] = open(filename, 'r')
        currLine[i] = filePointer[i].readline().strip()
        sep[i] = currLine[i].split(':')
        if sep[i][0] not in idHeap:
        	pq.heappush(idHeap,sep[i][0])

    cnt,termCnt,inCnt = 0,0,0
    f = open('./index/new'+str(inCnt),'wb')
    term = 'none'    	
    while(cnt<nFiles):
    	term = pq.heappop(idHeap)
    	termCnt+=1
    	cumIndex=''
    	for i in xrange(nFiles):
    		if finish[i]==0 :
	    		if sep[i][0]==term :
	    			cumIndex+=sep[i][1]
	    			currLine[i] = filePointer[i].readline().strip()
	    			if(not currLine[i]):
	    				finish[i]=1
	    				cnt+=1
	    				filePointer[i].close()
	    				# os.remove('./index/out'+str(i))
	        		else :
	        			sep[i] = currLine[i].split(':')
	        			if sep[i][0] not in idHeap:
	        				pq.heappush(idHeap,sep[i][0])
        print(term+':'+cumIndex, file=f)
        if(cnt==nFiles):
        	lastWord.append(term)
        if(termCnt==5000):
        	termCnt = 0        	
        	lastWord.append(term)
        	inCnt+=1
        	f.close()
        	f = open('./index/new'+str(inCnt),'wb')
    f.close()
    f = open('./index/offset.txt','wb')
    for i in lastWord :
    	print(i,file=f)
    f.close()
    return inCnt

total = mergeFiles(int(args[1]))
print(total)

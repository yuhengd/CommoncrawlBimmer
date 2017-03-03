import requests
import re
import argparse
import time
import json
import StringIO
import gzip
import csv
import codecs

from bs4 import BeautifulSoup, Comment

import sys
reload(sys)
sys.setdefaultencoding('utf8')

messageidset = set()

# parse the command line arg
if (len(sys.argv) !=2 ):
	print "Usage: commoncrawlerContentsJobArry.py <index 0-599>"
	exit(-1)
jobid = int (sys.argv[1])
subforum_id = jobid / 30
index_id = jobid % 30
# list of available indices
index_list = ["2013-20","2013-48","2014-10","2014-15","2014-23","2014-35","2014-41","2014-42","2014-49","2014-52","2015-06","2015-11","2015-14","2015-18","2015-22","2015-27","2015-32","2015-35","2015-40","2015-48","2016-07","2016-18","2016-22","2016-26","2016-30","2016-36","2016-40","2016-44","2016-50","2017-04"]
# use most recent index
#index_list = ["2016-22"]
#index_list = ["2015-22"]
subforum_list = ["http://f48.bimmerpost.com/forums/showthread.*","http://f87.bimmerpost.com/forums/showthread.*","http://f15.bimmerpost.com/forums/showthread.*","http://bmwi.bimmerpost.com/forums/showthread.*","http://www.2addicts.com/forums/showthread.*","http://f30.bimmerpost.com/forums/showthread.*","http://www.7post.com/forums/showthread.*","http://f80.bimmerpost.com/forums/showthread.*","http://f20.1addicts.com/forums/showthread.*","http://www.6post.com/forums/showthread.*","http://e89.zpost.com/forums/showthread.*","http://x3.xbimmers.com/forums/showthread.*","http://f10.m5post.com/forums/showthread.*","http://www.zpost.com/forums/showthread.*","http://f10.5post.com/forums/showthread.*","http://www.xbimmers.com/forums/showthread.*","http://www.e90post.com/forums/showthread.*","http://www.m3post.com/forums/showthread.*","http://e84.xbimmers.com/forums/showthread.*"]
print "subforum id is "+ str(subforum_id)
print "index id is "+ str(index_id)

#set domain and index
domain = subforum_list[subforum_id]
index_newlist = [index_list[index_id] ]
print "using index "+ str(index_newlist)
print "using subform host "+ domain
#global timestamp list

# grab visible text only
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)):
        return False
    return True

#
# Searches the Common Crawl Index for a domain.
#
def search_domain(domain):

    record_list = []
    
    print "[*] Trying target domain: %s" % domain
    
    for index in index_newlist:
        
        print "[*] Trying index %s" % index
        
        cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
        cc_url += "url=%s&matchType=domain&output=json" % domain
        
        response = requests.get(cc_url)
        
        if response.status_code == 200:
            
            records = response.content.splitlines()
            
            for record in records:
                record_list.append(json.loads(record))
            
            print "[*] Added %d results." % len(records)
            
    
    print "[*] Found a total of %d hits." % len(record_list)
    
    return record_list        

#
# Downloads a page from Common Crawl - adapted graciously from @Smerity - thanks man!
# https://gist.github.com/Smerity/56bc6f21a8adec920ebf
#
def download_page(record):

    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    #prefix = 'https://aws-publicdatasets.s3.amazonaws.com/'
    prefix = 'https://commoncrawl.s3.amazonaws.com/'
    
    # We can then use the Range header to ask for just this set of bytes
    #if connection error pass
    try:
	resp = requests.get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    	# The page is stored compressed (gzip) to save space
    	# We can extract it using the GZIP library
    	raw_data = StringIO.StringIO(resp.content)
    	#remove padding
    	'''raw_data.seek(-1,2)
    	pad_bytes = ord(raw_data.read(1))
    	raw_data.truncate(len(raw_data.getvalue())- pad_bytes)
    	raw_data.seek(0)'''
    	f = gzip.GzipFile(fileobj=raw_data)
    # What we have now is just the WARC response, formatted:
    # io error: not zipped file
    except:
	pass
    

    
    response = ""
    try:
	data = f.read() 
	if len(data):
	    try:
		warc, header, response = data.strip().split('\r\n\r\n', 2)
	    except:
		pass
    #data = raw_data.read() 
    #print for debug
    #print (str(data))
    except:
	pass    

    #response = ""
    
    '''if len(data):
        try:
            warc, header, response = data.strip().split('\r\n\r\n', 2)
        except:
            pass
    '''        
    return response

#
# Extract links from the HTML  
#
def extract_external_links(html_content,link_list):

    parser = BeautifulSoup(html_content)
        
    links = parser.find_all("a")
    
    if links:
        
        for link in links:
            href = link.attrs.get("href")
            
            if href is not None:
                
                if domain not in href:
                    if href not in link_list and href.startswith("http"):
                        print "[*] Discovered external link: %s" % href
                        link_list.append(href)

    return link_list

def extract_post_content(html_content,link_list):
	global messageidset
	parser = BeautifulSoup(html_content)
	#print (parser)
	threadmeta = parser.find(attrs={"property":"og:title"})
	threadname = ""
	if threadmeta :
		threadname = threadmeta['content'].encode('utf-8')
    #print threadname

	#link_list = []
	link_dict = {}
	
	# get post content
	posts = parser.find_all("div", {"class": "thePostItself"})

    #visible_posts = filter(visible, posts)
	visible_posts = posts
    #print (posts)
	if visible_posts:
		for post in visible_posts:
			link_list = []
	    	#print (post)
	    	#remove the quoted message
			try:
				quote = post.find("div", {'style': "font-style:italic"})
				quote.decompose()
				#post = post.find("div", {'class': "thePostItself"}).text
			except:
				pass
	    
	    	#post = post.find("div", {'class': "thePostItself"}).text
	    	#get the text content
			postid = post['id']
			if postid:
				if postid not in messageidset:
					messageidset.add(postid)
					post = post.text
					post=post.splitlines()
					for line in post:
						if 'Quote:' not in line and "Originally Posted by" not in line:	
							if line.strip():
								link_list.append(line)
		    	#if post not in link_list:
		    	#	link_list.append(post)
    
    	#link_dict[threadname] = link_list
    	#print (link_dict)
			if link_list:
				link_list.insert(0, threadname)
				link_dict[postid] = link_list
	#link_dict = {}
	#link_dict[threadname] = link_list
	return link_dict

record_list = search_domain(domain)
link_list   = []
link_dict = {}


#outputfp
domainbase = domain.replace("/","")
domainbase_index = domainbase+index_list[index_id]
print(domainbase_index)
with open("/scratch2/yuhengd/data/commoncrawl/%s-content-id.csv" % domainbase_index,'w') as outputfp:
	i = 0
	for record in record_list:
		#print record
    
		html_content = download_page(record)
		#if i == 0 and "og:title" in html_content:
		#if html_content.count('og:title') > 1:
			#titlecount = html_count('og:title')
			#print "title count is "+str(titlecount)
			#print html_content
    
		#print "[*] Retrieved %d bytes for %s" % (len(html_content),record['url'])
		if "og:title" in html_content:
			link_dict= extract_post_content(html_content,link_list)
	#print (link_list)
		a = csv.writer(outputfp,delimiter=',')
    
	#if (link_list):
	 #   for link in link_list:
		#print (link)
	#	a.writerow([link])
	
		if (link_dict):
	    #print(link_dict)
			try:
				for postid, postcontents in link_dict.iteritems():
				#for threadname, pageposts in link_dict.iteritems():
		    #print (threadname)
		    #print (link_dict[threadname])
					if postid and postcontents: 
					#if threadname:
						a.writerow([postid, postcontents])	    
						#a.writerow([threadname, pageposts])
			except:
				pass
		link_list = []
		link_dict = {}

	print "total messages included: "+ str(len(messageidset))

'''	
#print "[*] Total external links discovered: %d" % len(link_list)
datepattern = re.compile("\d{2}-\d{2}-\d{4}, \d{2}:\d{2} \w{2}")

domainbase = domain.replace("/","")
print(domainbase)
with open("/scratch2/yuhengd/data/commoncrawl/%s-timestamp.csv" % domainbase,'w') as outputfp1:
	b = csv.writer(outputfp1, delimiter=',')
	for record in record_list:
		html_content = download_page(record)
		# get timestamps
		timestamplist = re.findall(datepattern, html_content)
		if timestamplist:
			for timestamp in timestamplist:
				#print timestamp
				b.writerow([timestamp])

'''

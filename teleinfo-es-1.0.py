#!/usr/bin/env python
import argparse
import json
import requests
import serial
import sys
import time
import elasticsearch
import os.path
from pytz import timezone
from datetime import datetime

# before running install dependencies
#   sudo pip install requests elasticsearch pytz

# if there is no existing
# last file we do not send
do_not_send = False

# setting arguments of the program
parser = argparse.ArgumentParser(description='Process a teleinfo frame and send it to an ElasticSearch cluster')
parser.add_argument('-f', '--file', type=argparse.FileType('a'), default='teleinfo-es.json-'+time.strftime('%Y-%m-%d'), help='the file where to put collected data')
parser.add_argument('-l', '--last', default='teleinfo-es.last', help='the file where to put collected data')
parser.add_argument('-H', '--host', default='localhost', help='The hostname/ip of the ES cluster')
parser.add_argument('-P', '--port', type=int, default=9200, help='The port of the ES cluster')
parser.add_argument('-s', '--stty', default='/dev/ttyUSB0', help='The path to the serial /dev/tty*')
parser.add_argument('-i', '--index', default='teleinfo', help='The specific index name')
parser.add_argument('-t', '--timezone', default='Europe/Paris', help='The timezone we are in')

# parsing arguments
args = parser.parse_args()

if os.path.isfile(args.last):
  # read last doc
  try:
    last_doc = json.loads(open(args.last, 'r').read())
    print 'Loaded last data from file %s' % args.last
  except Exception, e:
    print 'Could not read last data from file: %s\n\t%s' % (args.last, repr(e))
    quit()
else:
  do_not_send = True

# one index per day
myindex = args.index+'-'+time.strftime('%Y-%m-%d')
# empty dict used to create the doc
doc = {}
str_indexes = ['MOTDETAT', 'OPTARIF', 'PTEC']
diff_indexes = ['BASE', 'HCHC', 'HCHP', 'EJPHN', 'EJPHM', 'BBRHCJB', 'BBRHPJB', 'BBRHCJW', 'BBRHPJW', 'BBRHCJR', 'BBRHPJR']

doc['timestamp'] = datetime.now(timezone(args.timezone))

es = elasticsearch.Elasticsearch([{'host': args.host, 'port': args.port}])
sr = serial.Serial(args.stty, 1200, bytesize=7, parity = 'E', stopbits=1)

trame = sr.readline()
# waiting for the end of the current frame
while '\x02' not in trame:
  trame = sr.readline()
# then it's a new frame !
trame = sr.readline()
# reading data until upcoming of a new frame
while '\x03' not in trame:
  trame = sr.readline()
  data = trame.split()
  # creating the doc attr:values
  try:
    if data[0] in str_indexes:
      doc[data[0]] = data[1]
    else:
      doc[data[0]] = int(data[1])
  except ValueError:
    doc[data[0]] = data[1]
  except Exception, e:
    print 'Error collecting teleinfo frame !\n\t%s' % (repr(e))
    quit()

if not do_not_send:
  for value in diff_indexes:
    if value in doc:
      if value not in last_doc:
        print 'Error last_doc was not containing %s value\n\t%s' % (value, last_doc)
        print 'Removing last_doc_file (%s) and quitting...' % args.last
        os.remove(args.last)
        quit()
      diff = abs(doc[value] - last_doc[value])
      if diff > 0:
        # on ajoute la valeur de diff que si elle est > 0
        doc[value+'-diff'] = diff

doc_data = json.dumps(doc, default=lambda o: o.astimezone(timezone(args.timezone)).replace(tzinfo=None).isoformat()+'Z' if hasattr(o, 'isoformat') else o)+'\n'
print 'Data has been read from serial !\n\t%s' % doc_data

try:
  last_doc_file = open(args.last, 'w+')
  last_doc_file.write(doc_data)
  print 'Wrote last data to file %s' % args.last
except Exception, e:
  print 'Could not write last data to file: %s\n\t%s' % (args.last, repr(e))
  quit()

if do_not_send:
  print 'Exiting as there were no previous data...'
  quit()

try:
  # creating the doc in the index
  res = es.index(index=myindex, doc_type='teleinfo-trame', body=doc)
  if res:
    print 'Sent teleinfo frame to ES http://%s:%i/%s' % (args.host, args.port, myindex)
except elasticsearch.ElasticsearchException as es1:
  print 'Error sending teleinfo frame to ES http://%s:%i/%s\n\t%s' % (args.host, args.port, myindex, es1.info)
finally:
  try:
    # dropping data to disk (can be used if we loose the HTTP connection)
    args.file.write(doc_data)
    print 'Wrote collected data to backup file %s' % args.file.name
  except Exception, e:
    print 'Could not backup data to file: %s\n\t%s' % (args.file, repr(e))

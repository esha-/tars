#!/usr/bin/env python

import os
import socket
import time
import yaml
import json
import msgpack
import sys
from os.path import dirname, join, realpath 

# Get the current working directory of this file.
# http://stackoverflow.com/a/4060259/120999
__location__ = realpath(join(os.getcwd(), dirname(__file__)))

# Add the shared settings file to namespace.
sys.path.insert(0, join(__location__, '..', 'src'))

import settings


"""#Create log file
i = 0
while os.path.exists('arena_log_'+str(i)) : 
    i+=1
log = open('arena_log_'+str(i),'w')
"""

# Connection to tarantool server admin_port (default 33015)
host = '127.0.0.1'
port = 33015

sock_tarantool = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_tarantool.connect((host, port))

data = "0"
arena_used = 0;

# Connection to skyline _port that listens  udp package (default 2025) 
listner_host = '127.0.0.1'
listner_port = 2025

# Get arena_used memory from box.slab.info()
request = 'box.slab.info()\n'

def seed():
 while True: 
  metric = 'arena_used'
  metric_set = 'unique_metrics'
  initial = int(time.time()) - settings.MAX_RESOLUTION
  
  mas_datapoint = []
  i = 0
  data_series = dict ({"results": []})
  print 'Write data'
  while i <= 1000:  
    i += 1 
    print i
    sock_tarantool.send(request)
    data = sock_tarantool.recv(1000000)
    data = yaml.load(data)
    data = data[0]
    val_arena_used = int(data.get('arena_used'))
    print ("arena_used=" + str(val_arena_used))
    dt = int(time.time()) - settings.MAX_RESOLUTION
    value = val_arena_used
    datapoint = [dt,value]
    data_series["results"].append(datapoint)
  with open('bas.json', mode='w') as f:  
        json.dump(data_series, f) 
  print "Send data" 
  
  with open(join(__location__, 'bas.json'), 'r') as f:
      data = json.loads(f.read())
      series = data['results']
      sock_listner = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      for datapoint in series:
          datapoint[0] = initial
          initial +=1
          packet = msgpack.packb((metric, datapoint))
          #log.write(packet + '\n') 
          sock_listner.sendto(packet, (socket.gethostname(), listner_port))
  print "End"

if __name__ == "__main__":
  
    seed()
    
  #  sock_tarantool.close()


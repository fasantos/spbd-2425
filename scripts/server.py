#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 00:51:20 2019

@author: nmp
"""

import socket
import datetime
import time
import sys
import threading

import sys
import argparse
import dataclasses, json
from dataclasses import dataclass
from json import dumps


@dataclass
class EnergyLine:
  date: str
  sensor: str
  energy : float
	
def parse( line ):
  tokens = line.strip().split(';')
  if len( tokens ) == 3:
  	_date = tokens[0]
  	_sensor = tokens[1]
  	_energy = float(tokens[2])
  	
  	return EnergyLine(date = _date, sensor = _sensor, energy = _energy)
  else:
  	return None
  
def current_lines(lines) :
  result = []
  try:
      for line in lines:
          parts = line.split(';')
          current_date = datetime.now()
          current_year = current_date.year
          current_month = current_date.month
          updated_date = datetime \
            .strptime(parts[0], "%Y-%m-%d %H:%M:%S") \
            .replace(year=current_year, month=current_month)

          parts[0] = updated_date.strftime("%Y-%m-%d %H:%M:%S")
          result.append(';'.join(parts))
          
      return result
  except Exception as err:
            print(err)
  	

def publish(conn, lines, speedup) :
    try:         
        firstLineTime = -1
        firstWallTime = -1
        for line in lines:
            try:
                parts = line.split(';')
                lineTime = datetime.datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
                if firstLineTime == -1 :
                    firstLineTime = lineTime
                    firstWallTime = datetime.datetime.now()
                    
                deltaLineTime = lineTime - firstLineTime
                
                deltaLineTimeS = (lineTime - firstLineTime) / datetime.timedelta(microseconds=1) / 1000000.0
                
                #print('line relative time: : {} secs'.format(deltaLineTimeS))

                wallTime = datetime.datetime.now()
                deltaWallTimeS = (wallTime - firstWallTime) / datetime.timedelta(microseconds=1) / 1000000.0
                

                #print('wall relative time: : {} secs'.format(deltaWallTimeS))
                
		    
                delay = (deltaLineTimeS/speedup - deltaWallTimeS)

                print( 'wall time: {}, delay: {}, {}'.format(deltaWallTimeS, delay, 1.0/delay))
		
                if delay > 0 :
                    time.sleep( delay )
                        
                dt = parse( line )
                if dt != None:
                	jsonline = json.dumps( dataclasses.asdict(dt) ) + '\n'
                	conn.sendall( jsonline.encode())

            except Exception as err:
                print(err)
                
    except Exception as err:
            print(err)

HOST = "0.0.0.0"
PORT = 7777 

parser = argparse.ArgumentParser(description='dataset publisher...')
parser.add_argument('--filename', type=str, default='energy-readings.csv', help='dataset filename ') 
parser.add_argument('--speedup', type=int, dest='speedup', default=60, help='time speedup factor (default: 60)')

args = parser.parse_args()
print(args)

f = open(args.filename, "r")
lines = current_lines(f.readlines())
f.close()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(5)
while True:
    conn, addr = s.accept()
    th = threading.Thread(target=publish, args=(conn,lines,args.speedup))
    th.start()    
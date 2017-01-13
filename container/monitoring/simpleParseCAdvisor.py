#!/usr/bin/python
import argparse

import pprint 
import requests
import json
import socket

import sys



def generatePoint(measurement, time, fields, tags = None):
   point =  {
            "measurement": measurement,
            "time": time,
            "fields": fields
        }
   if tags:
     point['tags'] = tags
   return point

def fetchCpuPoints(stats):

  cpu_measurement = stats['cpu']
  fields = { 'load_average' : cpu_measurement['load_average'] }
  usage = cpu_measurement['usage']
  for cpuId, cpuUsage in enumerate(usage['per_cpu_usage']):
    fields['per_cpu_usage_%s'%cpuId] = cpuUsage
  for v in ['system', 'total', 'user']:
    fields[v] = usage[v]
  return generatePoint('cpu', stats['timestamp'],fields)


def defaultFetch(dataDict):
  fields = {}
  for key,value in dataDict.iteritems():
    if isinstance(value, dict):
      #print "CHRIS %s"%dataDict
      continue
      fields.update( dict( ("%s_%s"%(key,k1), v1)for k1, v1 in value.iteritems()))
    else:
      fields[key] = value

  return fields

def fetchMemoryPoints(stats):
  memory_measurements = stats['memory']
  fields = defaultFetch(memory_measurements)
  return generatePoint('memory', stats['timestamp'], fields)


def fetchNetworkPoints(stats):
  points = []
  network_measurements = stats['network']
  # We always only have one interface, so the total is what we want
  interfaces = network_measurements.pop('interfaces')
  if len(interfaces) >= 1:
    for interface in interfaces:
      int_name = interface.pop('name')
      points.append(generatePoint('network', stats['timestamp'], defaultFetch(interface), tags = {'name' : int_name}))
  network_measurements.pop('name')
  fields = defaultFetch(network_measurements)
  points.append(generatePoint('network', stats['timestamp'], fields))
  return points

def fetchFilesystemPoints(stats):
  points = []
  fs_measurements = stats['filesystem']
  for fs in fs_measurements:
    device = fs.pop('device')
    if 'docker' in device:
      continue
    points.append(generatePoint('filesystem', stats['timestamp'], defaultFetch(fs), tags = {'device' : device}))
  return points

def fetchTaskPoints(stats):
  return generatePoint('tasks', stats['timestamp'], defaultFetch(stats['task_stats']))



def collectData(target):
  
  if not target:
    target = 'localhost'
    hostname = socket.gethostname().split('.')[0]
  else:
    hostname = target
  
  d = requests.get('http://%s:8080/api/v1.3/docker'%target).json()
  machine_stats = requests.get('http://%s:8080/api/v1.3/containers'%target).json()['stats']
  
  collected_data = [] # list of (points, tags)

  for container, container_data in d.iteritems():
    if not any('mesos' in alias for alias in  container_data['aliases']):
      continue 
    dirac_component = container_data.get('labels',{}).get('dirac_component')
    if not dirac_component:
      continue
    
    docker_id = container_data['id']
    
    container_stats = container_data['stats']
    points = []
    for stats in container_stats:
      points.append(fetchCpuPoints(stats))
      points.append(fetchMemoryPoints(stats))
      points.extend(fetchNetworkPoints(stats))   
      points.append(fetchTaskPoints(stats))
      break
    collected_data.append( (points, { 'host' : hostname, 'dirac_component' : dirac_component, 'docker_id': docker_id } ) )
  
  points = []
  for stats in machine_stats:
    points.append(fetchCpuPoints(stats))
    points.append(fetchMemoryPoints(stats))
    points.extend(fetchNetworkPoints(stats))
    points.append(fetchTaskPoints(stats))
    points.extend(fetchFilesystemPoints(stats))
  collected_data.append((points, { 'host' : hostname }))

  return collected_data

def writeDataInInflux(collected_data, host = 'host', port = 8086, username = 'user', password = 'password', database = 'database', ssl = True ):
  from influxdb import InfluxDBClient
  client = InfluxDBClient(host = host, port =  port, username = username, password = password, database = database, ssl = ssl)

  for points, tags in collected_data:
    client.write_points(points, tags = tags)

if __name__ == '__main__':
  progDescription = """ Collect data from cadvisor rest api.
                    """
  parser = argparse.ArgumentParser(description=progDescription, add_help=True)
  parser.add_argument('-t', '--target',
                      help='Target hostname. Default to localhost',
                      dest='target',
                      default=None)

  parser.add_argument('-c', '--config',
                      help='Json config file to use',
                      dest='config', required = True,
                      default=None)

  parser.add_argument('-i', '--influx',
                      help='Send the collected data to influxdb',
                      dest='influx', action='store_true')

  parser.add_argument('-q', '--quiet',
                      help='Do not print the collected data',
                      dest='quiet', action='store_true')

  args = parser.parse_args()

  print args
  
  with open(args.config, 'r') as configFile:
    config = json.load(configFile)

  collected_data = collectData(args.target)
  if not args.quiet:
    for points, tags in collected_data:
      pprint.pprint(tags)
      pprint.pprint(points)
      print '\n'*2
  if args.influx:
   writeDataInInflux(collected_data, 
                     host = config['host'],
                     port = config['port'],
                     username = config['username'],
                     password = config['password'],
                     database = config['database'],
                     ssl = config['ssl'])

 



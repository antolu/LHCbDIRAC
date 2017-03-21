#!/usr/bin/env python

# This script is meant to generate marathon compatible json file from few basic informations.
# The output json file can be copied to the marathon web interface, or sent via the rest interface.


import argparse
import json
import logging
import pprint
import sys
import os

# Default number of instances if not specified
defaultInstances = 1
# Default number of CPU to use if not specified
defaultCpu = 0.1
# Default memory (in MB) to use if not specified
defaultMem = 200

def readTemplateFile(tplFileName):
  """ Read the template file """
  with open(tplFileName, 'r') as f:
    return ''.join(line.strip('\n') for line in  f.readlines())



def generateConfig(tplService, component, port, version, cpu, mem, instances ):
  """ Generate a JSON configuration file compatible with marathon from basic
      info and template
      
      :param tplService: the template string in which to replace the values
      :param component: DIRAC component (i.e. DataManagement/FileCatalog)
      :param port: port to expose
      :param version: DIRAC version
      :param cpu: number of CPU to dedicate to the container (default 0.1)
      :param mem: memory to dedicate to the container (default 200M)
      :param instances: number of instances to run (default 1)
      
      :return: JSON config file
  """
  
  cpu = cpu if cpu else defaultCpu
  mem = mem if mem else defaultMem
  instances = instances if instances else defaultInstances
  
  serviceId = component.lower()
  
  configStr = tplService%{ 'ID' : serviceId,
                           'CPU' : cpu,
                           'MEM' : mem,
                           'INSTANCES': instances,
                           'PORT' : port,
                           'COMPONENT' : component,
                           'VERSION' : version}
  configJson = json.loads(configStr)

  return configJson

if __name__ == '__main__':
  logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
  
  progDescription = """ Management script to generate marathon services based on a template
                    """
                    

  parser = argparse.ArgumentParser(description=progDescription, add_help=True)
  
  parser.add_argument('-v', '--version', required = True,
                      help='Version of LHCbDIRAC (e.g. v8r4p5)',
                      dest='version')

  parser.add_argument('-i', '--input',
                      help="""JSON file containing a list of services to generate.
                              In that case, --dest should be a directory.
                              This is incompatible with --cpu, --mem, --instances, --port, --component,
                              as the JSON file contains all these information.""",
                      dest='input')

  parser.add_argument('-t', '--template', required=True,
                      help='File to use as template',
                      dest='template')

  parser.add_argument('-c', '--component',
                      help='Component to generate (e.g. DataManagement/FileCatalog)',
                      dest='component')

  parser.add_argument('-p', '--cpu',
                      help='Number of core to dedicate (e.g. 0.1)',
                      dest='cpu')

  parser.add_argument('-m', '--mem',
                      help='Number of memory (in MB) to dedicate (e.g. 0.1)',
                      dest='mem')

  parser.add_argument('-n', '--instances',
                      help='Number of instances to run',
                      dest='instances')

  parser.add_argument('-o', '--port',
                      help='Port of the service',
                      dest='port')

  parser.add_argument('-d', '--dest',
                      help='Destination where to store the output. If --input, --dest should be a directory. If not set, just prints',
                      dest='dest')

  args = parser.parse_args()
  
  # if using an input file
  if args.input:
    # we should not use any service specific option
    if any([args.cpu, args.mem, args.instances, args.port, args.component]):
      logging.error("--input incompatible with --cpu, --mem, --instances, --port, --component")
      parser.print_help()
      sys.exit(1)
    
    # if set, destination should be a directory
    if args.dest and not os.path.isdir(args.dest):
      logging.error("--dest should be a directory with --input")
      parser.print_help()
      sys.exit(1)
    
    
  # if not using input file
  if not args.input:
    # all the necessary info should be there
    if not all([args.port, args.component]):
      logging.error("--port and --component are mandatory")
      parser.print_help()
      sys.exit(1)
      
    # if set, destination should be a file
    if args.dest and os.path.isdir(args.dest)  :
      logging.error("--dest should be file")
      parser.print_help()
      sys.exit(1)
  
  
  servicesCfgDict = {}
  
  tplService = readTemplateFile(args.template)
  
  if not args.input:
    serviceJSON = generateConfig(tplService, args.component, args.port, args.version, args.cpu, args.mem, args.instances )
    servicesCfgDict[args.component] = serviceJSON
  else:
    with open(args.input, 'r') as f:
      allServices = json.load(f)
    
    for system, services in allServices.iteritems():
      for service, serviceOptions in services.iteritems():
        component = '%s/%s'%(system, service)
      
        serviceJSON = generateConfig(tplService, component, serviceOptions['port'], args.version, serviceOptions.get('cpu'), serviceOptions.get('mem'), serviceOptions.get('instances') )
        servicesCfgDict[component] = serviceJSON
    
  
  if not args.dest:
    for component, serviceJSON in servicesCfgDict.iteritems():
      print json.dumps(serviceJSON, indent = 2)
  else:
    if not args.input:
      with open(args.dest, 'w') as destFile:
        json.dump(servicesCfgDict[args.component], destFile, indent = 2)
    else:
      for component, serviceJSON in servicesCfgDict.iteritems():
        fn = component.replace('/', '_')
        with open(os.path.join(args.dest, fn) + '.json', 'w') as destFile:
          json.dump(serviceJSON, destFile, indent = 2)
  #generateConfig(readTemplateFile('service_template.json'), 'RequestManagement/ReqManager', 0.2, 0.1, 1, 9140, 'v8r5')

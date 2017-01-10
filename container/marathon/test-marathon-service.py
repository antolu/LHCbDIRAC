#!/usr/bin/env python

import json
import docker
import time
import argparse




def testConfigFile(filename):
  """ Read a json config file for marathon, and run it interactively """

  with open(filename, 'r') as f:
    jsonConfig = json.load(f)


  image = jsonConfig['container']['docker']['image']
  command = jsonConfig['args']

  volumes = {}

  for v in jsonConfig['container']['volumes']:
    volumes[v['hostPath']] = { 'bind' : v['containerPath'], 'mode' : v['mode'].lower()}

  port = jsonConfig['container']['docker']['portMappings'][0]['containerPort']
  ports = { port : port }



  client = docker.from_env()

  # Pull the image first
  for line in client.pull(image, stream = True):
    print json.dumps(json.loads(line), indent = 2)

  # Create the container
  container = client.create_container(image, name = 'test_container', command = command, detach = True, ports = ports.keys(), volumes = volumes.keys(), host_config = client.create_host_config(binds = volumes, port_bindings = ports), stdin_open = True, tty = True)

  containerId = container['Id']
  response = client.start(container = containerId)
  print "Pausing..."
  time.sleep(5)
  response = client.logs(container = containerId, stream = False, follow = False, stdout = True, tail = 100)
  print response
  print "Stopping..."
  client.stop(containerId)
  print "Removing..."
  client.remove_container(container = containerId, v = True)


if __name__ == '__main__':

  progDescription = """ Testing script for marathon config of DIRAC services.
                    """
  parser = argparse.ArgumentParser(description=progDescription, add_help=True)
  parser.add_argument('-i', '--input', required=True,
                      help='Path to the marathon json config file',
                      dest='input')

  args = parser.parse_args()

  testConfigFile(args.input)

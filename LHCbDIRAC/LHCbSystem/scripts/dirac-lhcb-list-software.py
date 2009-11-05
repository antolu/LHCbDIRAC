#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-lhcb-list-software
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import sys,string
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gConfig

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<Optional System Configuration To Query For>]' %(Script.scriptName)
  DIRAC.exit(2)

def printSoftware(package,packageArch):
  adj = 30
  print package.split('.')[0].ljust(adj)+package.split('.')[1].ljust(adj)+string.join(packageArch,',').ljust(adj)

def printHeader(header):
  print '=========> %s ' %header

if len(args) > 1:
  usage()

softwareDistribution = gConfig.getOptionsDict('/Operations/SoftwareDistribution')
if not softwareDistribution['OK']:
  print 'ERROR: Could not get values for /Operations/SoftwareDistribution section with message:\n%s' %(result['Message'])
  DIRAC.exit(2)

software = softwareDistribution['Value']
systemConfigs = software.keys()
systemConfigs.remove('Active')
systemConfigs.remove('Deprecated')

warn = True
if len(args)==1:
  warn = False
  systemConfigs = [args[0]]

active = software['Active'].replace(' ','').split(',')
active.sort()
deprecated = software['Deprecated'].replace(' ','').split(',')
deprecated.sort()

if not warn:
  printHeader('Active LHCb Software For System Configuration %s' %(args[0]))
else:
  printHeader('Active LHCb Software For All System Configurations')

for package in active:
  packageArch = []
  for systemConfig in systemConfigs:
    if package in software[systemConfig]:
      packageArch.append(systemConfig)
  if packageArch:
    printSoftware(package,packageArch)
  else:
    if warn:
      print 'WARNING: %s %s is not defined for any system configurations' %(package.split('.')[0],package.split('.')[1])

if warn:
  printHeader('Deprecated LHCb Software')
  print '%s' %(string.join(deprecated,', '))

DIRAC.exit(0)
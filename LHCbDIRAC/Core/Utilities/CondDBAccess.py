########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Core/Utilities/CondDBAccess.py $
# Author: Stuart Paterson
########################################################################

"""  The CondDB access module allows to perform a nasty hack to disable
     the LFC lookup in CORAL used by Gaudi.
     
     Now that the nasty hack has become the default way to run there have
     been some improvements.  By storing the access information in the 
     DIRAC CS we avoid using AppConfig files or the LFC in the course of
     everyday operations and can have fine-grained control over the list
     of active CondDB instances via the standard CS interface.
"""

__RCSID__ = "$Id$"

import random
import shutil
import string,os,re,sys
import xml.etree.ElementTree as ETree
from xml.etree.ElementTree import tostring

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Os import sourceEnv

gLogger = gLogger.getSubLogger( "CondDBAccess" )

authName = 'authentication.xml'
lookupName = 'dblookup.xml'
connectionStrings = {'CondDB':'/lhcb_conddb', 'CondDBOnline':'/lhcb_online_conddb'}

#############################################################################
def getCondDBFiles(localSite='',directory='',forceSite=''):
  """ Function to set up the necessary CORAL XML files to bypass LFC access.
      
      This function draws it's information from the DIRAC CS therefore 
      removing the necessity to persist information in the LFC.  The following
      CS structure is assumed for the CondDB access:
      
      Resources{
        CondDB{
          LCG.CERN.ch{
                      Status = <Active / InActive>
                      Connection = <connection string>
                      Username = <user name>
                      Password = <password>
                    }
      }
      
      As we have NIKHEF and SARA sharing a DB instance an N->1 mapping is allowed
      for site -> connection strings.
  """
  #local site is just to test the normal behaviour of the utility at 
  #different site names e.g. seeing the ordered list
  if not localSite:
    localSite = gConfig.getValue('/LocalSite/Site','')

  #the XML files have to be in the local directory by default    
  if not directory:
    directory = os.getcwd()

  localAuth = '%s/%s' %(directory,authName)
  localLookup = '%s/%s' %(directory,lookupName)
    
  sitesList = gConfig.getSections('/Resources/CondDB',[])
  if not sitesList['OK']:
    gLogger.error(sitesList)
    return S_ERROR('Could Not Determine CondDB Sites')
  
  sitesList = sitesList['Value']
  connections = {}
  for site in sitesList:
    status = gConfig.getValue('/Resources/CondDB/%s/Status' %(site),'InActive')
    if status.lower()=='active':
      connStr = gConfig.getValue('/Resources/CondDB/%s/Connection' %(site),'')
      if connections.has_key(connStr):
        existing = connections[connStr]
        existing.append(site)
        connections[connStr]=existing
      else:
        connections[connStr]=[site]

  #if a single site is being forced (for testing for example) this is only site 
  #added to the file and the status is *not* checked
  if forceSite:
    fsCon=gConfig.getValue('/Resources/CondDB/%s/Connection' %(forceSite),'')
    if not fsCon:
      gLogger.error('Could not find connection string for /Resources/CondDB/%s/Connection' %(forceSite))
      return S_ERROR('Could not find connection string for specified CondDB site "%s"' %(forceSite))
    gLogger.info('Forcing CondDB site to "%s" as specified' %(forceSite))
    connections = {}
    connections[fsCon]=[forceSite]

  if not connections:
    return S_ERROR('No Active CondDB Instances Found')

  #check if local site is in the list and active
  prependLocal = ''
  connectionsList = connections.keys()

  if not forceSite:
    for conString,site in connections.items():
      if localSite in site:
        connectionsList.remove(conString)
        prependLocal=conString
    
  #shuffle the remaining sites (if forceSite was specified there is only 1)
  random.shuffle(connectionsList)
  orderedConnections = connectionsList

  orderedSites = []
  for cl in connectionsList:
    orderedSites+=connections[cl] 
  
  if not forceSite:
    if prependLocal:
      orderedConnections = [prependLocal]+connectionsList
      orderedSites = [localSite]+orderedSites
    else:
      gLogger.info('No local CondDB instance found for site "%s", will randomly shuffle active instances' %(localSite))
    
  gLogger.info('CondDB connections will be attempted in the following order: %s' %(string.join(orderedSites,', ')))

  #first create the lookup file using ordering as above
  lookup = ETree.Element('servicelist')
  for cs,app in connectionStrings.items():
    service = ETree.SubElement(lookup,'logicalservice',{'name':cs})
    for siteCon in orderedConnections:
      site = '%s%s' %(siteCon,app)
      ETree.SubElement(service,'service',{'accessMode':'readonly','authentication':'password','name':site})

  writeXMLFile(lookup,lookupName)

  #now the auth file
  auth = ETree.Element('connectionlist')
  for siteCon in orderedConnections:
    sites = connections[siteCon]
    if len(sites)>1:
      gLogger.verbose('Sites %s have the same connection string' %(string.join(sites,', ')))
    site = sites[0]
    user = gConfig.getValue('/Resources/CondDB/%s/Username' %(site),'')
    passPhrase = gConfig.getValue('/Resources/CondDB/%s/Password' %(site),'')
    if not user or not passPhrase:
      gLogger.warn('Excluding %s CondDB since not all info is available: Username "%s", Password "%s"' %(user,passPhrase))
      continue    
    
    for conddb in connectionStrings.values():
      siteConnection = '%s%s' %(siteCon,conddb)
      connection = ETree.SubElement(auth,'connection',{'name':siteConnection})
      passDictList = [{'name':'user','value':user},{'name':'password','value':passPhrase}]
      for p in passDictList:
        ETree.SubElement(connection,'parameter',p)
  
      reader = ETree.SubElement(connection,'role',{'name':'reader'})
      for p in passDictList:
        ETree.SubElement(reader,'parameter',p)

  writeXMLFile(auth,authName)

  return S_OK([localLookup,localAuth])

#############################################################################
def indent(elem, level=0):
  """ Indent an ElementTree instance to allow pretty print.
      Code taken from http://effbot.org/zone/element-lib.htm
  """
  i = "\n" + level*"  "
  if len(elem):
    if not elem.text or not elem.text.strip():
      elem.text = i + "  "
    if not elem.tail or not elem.tail.strip():
      elem.tail = i
    for child in elem:
      indent(child, level+1)
    if not child.tail or not child.tail.strip():
      child.tail = i
    if not elem.tail or not elem.tail.strip():
      elem.tail = i
  else:
    if level and (not elem.tail or not elem.tail.strip()):
      elem.tail = i

#############################################################################
def writeXMLFile(eltTree,fname):
  """ Simple method to write an indented XML file given an ElementTree object
      and output file name.
  """
  outputFile = open(fname,'w')
  outputFile.write('<?xml version="1.0"?>\n')
  indent(eltTree)
  ETree.ElementTree(eltTree).write(outputFile)
  gLogger.debug('====> CondDB Access File %s\n%s' %(fname,tostring(eltTree)))
  outputFile.close()
  return S_OK(fname)

#############################################################################
def getCondDBFilesOld(appConfigRoot,localSite='',directory=''):
  """ Function to set up the necessary CORAL XML files to bypass LFC access.
      Any project environment will pick up the latest AppConfig.
      Relies on the following APPCONFIG conventions:

      $APPCONFIGROOT/conddb/dblookup-<SITE>.xml
      $APPCONFIGROOT/conddb/authentication.xml
      $APPCONFIGOPTS/UseOracle.py
      $APPCONFIGOPTS/DisableLFC.py - Trigger in GaudiApplication for getting these files
  """
  if not localSite:
    localSite = DIRAC.gConfig.getValue('/LocalSite/Site','LCG.CERN.ch')
    
  if not directory:
    directory = os.getcwd()
    
  if not os.path.exists(appConfigRoot):
    return S_ERROR('APPCONFIGROOT ( %s ) does not exist!' %(appConfigRoot))    

  ambiguousString = ['p', 'a', 's', 's', 'w', 'o', 'r', 'd', 'c', 'r', 'a', 'z', 'i', 'n', 'e', 's', 's']
  ambiguous = string.join(ambiguousString).replace(' ','').replace('craziness','')
  otherString = ['c', 'o', 'n', 'd', 'd', 'b', 'u', 's', 'e', 'r']
  other = string.join(otherString).replace(' ','').replace('conddb','')
    
  condDBSite = localSite.split('.')[1]
  gLogger.verbose('Running at site: %s, CondDB site is: %s' %(localSite,condDBSite))
  lookupFile = '%s/conddb/dblookup-%s.xml' %(appConfigRoot,condDBSite)
  defaultLookup = '%s/conddb/dblookup-CERN.xml' %(appConfigRoot)
  if not os.path.exists(lookupFile):
    gLogger.error('Could not find %s, reverting to CERN file' %lookupFile)
    lookupFile = defaultLookup
    if not os.path.exists(defaultLookup):
      gLogger.error('Could not find %s' %defaultLookup)
      return S_ERROR('Missing %s' %defaultLookup)

  #copy local so as not to read from the shared area
  localLookup = '%s/dblookup.xml' %(directory)
  if not os.path.exists(localLookup):
    shutil.copy(lookupFile,localLookup)
  else:
    gLogger.debug('Local lookup file already present: %s' %localLookup)  

  authFile = '%s/conddb/authentication.xml' %(appConfigRoot)
  if not os.path.exists(authFile):
    gLogger.error('Could not find %s' %authFile)
    return S_ERROR('Missing %s' %(authFile))
  
  localAuth = '%s/authentication.xml' %(directory)
  if not os.path.exists(localAuth):
    shutil.copy(authFile,localAuth)
  else:
    gLogger.debug('Local authorization file already present: %s' %localAuth)
    return S_OK([localLookup,localAuth])
  
  fopen = open(localAuth,'r')
  authString = fopen.read()
  fopen.close()

  fopen = open(localAuth,'w')
  fopen.write(authString.replace('attribute',ambiguous).replace('aid',other))
  fopen.close()

  return S_OK([localLookup,localAuth])

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
#!/usr/bin/env python
'''Script to run Gaudi application'''

from os import curdir, system, environ, pathsep, sep, getcwd
from os.path import join
import sys

def prependEnv(key, value):
    if environ.has_key(key): value += (pathsep + environ[key])
    environ[key] = value

# Main
if __name__ == '__main__':

    prependEnv('LD_LIBRARY_PATH', getcwd() + '/lib')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/python')
    prependEnv('PYTHONPATH', getcwd() + '/InstallArea/x86_64-slc5-gcc46-opt/python')

    rc = system("""gaudirun.py -T options.pkl data-wrapper.py""")/256

    # Parsed XMLSummary data extraction methods
def xmldatafiles(xmlsummary):
  '''return a dictionary of the files the xmlsummary saw as input'''
  returndict={}
  for file in xmlsummary.file_dict()['input'].values():
    if file.attrib('status') in returndict:
      returndict[file.attrib('status')].update([file.attrib('name')])
    else:
      returndict[file.attrib('status')]=set([file.attrib('name')])
  return returndict
def xmldatanumbers(xmlsummary):
  '''return a dictionary of the number of files the xmlsummary saw as input'''
  returndict={}
  for file in xmlsummary.file_dict()['input'].values():
    if file.attrib('status') in returndict:
      returndict[file.attrib('status')]=returndict[file.attrib('status')]+1
    else:
      returndict[file.attrib('status')]=1
  return returndict
def xmlskippedfiles(xmlsummary):
  '''get all skipped files from xml'''
  filedict=xmldatafiles(xmlsummary)
  skippedfiles=set()
  for stat in ['none','fail']:
    if stat in filedict:
      skippedfiles.update(filedict[stat])
  return skippedfiles
def events(xmlsummary):
  '''given an XMLSummary object, will return the number of events input/output'''
  ad=xmlsummary.file_dict()
  evts={}
  for type in ad.keys():
    if type not in evts:
      evts[type]=0
    for file in ad[type].keys():
      if type=='input' and ad[type][file].attrib('status')=='mult':
        print 'Warning, processed file ', ad[type][file].attrib('name'), 'multiple times'
      if ad[type][file].attrib('GUID')==file:
        #print 'ignoring'
        continue
      else:
        evts[type]+=ad[type][file].value()
  return evts
def lumi(xmlsummary):
  '''given an XMLSummary object, will return the integrated luminosity'''
  #  print xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()[0],'+/-',xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()[2]

  lumiDict = dict( zip( xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].attrib('format'),
                        xmlsummary.counter_dict()['lumiCounters']['IntegrateBeamCrossing/Luminosity'].value()
                        )
                   )
  return '"%s +- %s"' % (lumiDict['Flag'], lumiDict['Flag2'])
def activeSummaryItems():
  activeItems = {'lumi'           :lumi,
                 'events'         :events,
                 'xmldatafiles'   :xmldatafiles,
                 'xmldatanumbers' :xmldatanumbers,
                 'xmlskippedfiles':xmlskippedfiles
                 }
  return activeItems

# XMLSummary parsing
import os, sys
if 'XMLSUMMARYBASEROOT' not in os.environ:
    sys.stderr.write("'XMLSUMMARYBASEROOT' env var not defined so summary.xml not parsed")
else:
    schemapath  = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'xml/XMLSummary.xsd')
    summarypath = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'python/XMLSummaryBase')
    sys.path.append(summarypath)
    import summary

    outputxml = os.path.join(os.getcwd(), 'summary.xml')
    if not os.path.exists(outputxml):
        sys.stderr.write("XMLSummary not passed as 'summary.xml' not present in working dir")
    else:
        try:
            XMLSummarydata = summary.Summary(schemapath,construct_default=False)
            XMLSummarydata.parse(outputxml)
        except:
            sys.stderr.write("Failure when parsing XMLSummary file 'summary.xml'")

        # write to file
        with open('__parsedxmlsummary__','w') as parsedXML:
            for name, method in activeSummaryItems().iteritems():
                try:
                    parsedXML.write( '%s = %s\n' % ( name, str(method(XMLSummarydata)) ) )
                except:
                    parsedXML.write( '%s = None\n' % name )
                    sys.stderr.write('XMLSummary error: Failed to run the method "%s"\n' % name)


    sys.exit(rc)

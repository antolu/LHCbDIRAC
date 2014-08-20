#!/usr/bin/env python
""" fetch events specified in 'request' file
"""

__RCSID__ = "$Id$"

import os
import sys
from LHCbConfig import ApplicationMgr, INFO, InputCopyStream
import GaudiPython

from LHCbDIRAC.TransformationSystem.Utilities.GridCollector.Request import Request


def process_pfn( sel, evt, appMgr, PFN, positions ):
  assert PFN is not None, "PFN is None"
  print "process_PFN: %s" % PFN
  sel.open( [PFN] )

  cur_pos = 0
  rv = 0
  for pos in map( int, positions ):
    offset = pos - cur_pos - 1
    assert offset >= 0, "wrong offset: %d" % offset
    if offset > 0:
      appMgr.run( offset )
      cur_pos += offset
    appMgr.run( 1 )
    cur_pos += 1
    if evt['Rec/Header']:
      print 'write event', evt['Rec/Header']
      appMgr.algorithm( 'InputCopyStream' ).execute()
    else:
      rv = 1
      print "no event header ( %d )" % pos
  return rv


def fetch_event( pfn_request_list, output_file ):
  rv = 0
  InputCopyStream().Output = "DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='REC' " % output_file
  appConf = ApplicationMgr( OutputLevel = INFO, AppName = 'fetch_event' )
  appConf.OutStream = [InputCopyStream()]
  appMgr = GaudiPython.AppMgr()
  sel = appMgr.evtsel()
  evt = appMgr.evtsvc()
  appMgr.algorithm( 'InputCopyStream' ).Enable = False

  for PFN, positions in pfn_request_list:
    rv += process_pfn( sel, evt, appMgr, PFN, positions )
  return rv


def init():
  if len( sys.argv ) < 3:
    print "Usage: fetch_event.py request_file output_file"
    exit( 1 )
  input_file = sys.argv[1]
  output_file = sys.argv[2]
  assert os.path.exists( input_file ), "no file: " + input_file
  print "EventFetch: %s -> %s" % ( input_file, output_file )
  return input_file, output_file


def main():
  input_file, output_file = init()
  request = Request( req_file = input_file )
  print request
  assert request.pfn_req_list is not None, "empty PFN request list"
  return fetch_event( request.pfn_req_list, output_file )


if __name__ == '__main__':
  main()

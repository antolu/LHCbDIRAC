#!/usr/bin/env python
""" fetch events specified in 'request' file
"""

__RCSID__ = "$Id$"

import os
import sys
from itertools import imap
import logging
from LHCbConfig import ApplicationMgr, INFO, InputCopyStream #pylint: disable=F0401
import GaudiPython #pylint: disable=F0401
#from LHCbDIRAC.TransformationSystem.Utilities.GridCollector.Request import Request
from Request import Request, normalize_lfns


def process_pfn( sel, evt, appMgr, PFN, positions, stop_on_error=False ):
  """Downloads events on specified positions in a PFN.

  Downloads events on specified positions in a PFN. Is guranteed to
    process positions in ascending order.

  Args:
    sel: an GaudiPython.appMgr.evtsel() instance.
    evt: an GaudiPython.appMgr.evtsvc() instance.
    appMgr: a GaudiPython.appMgr instance.
    PFN: target PFN
    positions: an iterable with positions in the PFN
    stop_on_error: if False, will continue on an invalid event,
      and return 1, if True, will stop on an invalid event and return,
      a dict {"success": False, "position": <postion_of_the_invalid_event>}

  Returns:
    0 on success
    1 if at least some events couldn't be read and stop_on_error is False
    {"success": False, "position": <postion_of_the_invalid_event>}
      if encountered an invalid event and stop_on_error is True. The events
      on lower positions have been successfully processed.
  """
  assert PFN is not None, "PFN is None"
  logging.info("process_PFN: %s" % PFN)
  sel.open( [PFN] )
  cur_pos = 0
  rv = 0
  for pos in sorted(imap( int, positions )):
    offset = pos - cur_pos - 1
    assert offset >= 0, "wrong offset: %d" % offset
    if offset > 0:
      ret = appMgr.run( offset )
      cur_pos += offset
    appMgr.run( 1 )
    cur_pos += 1
    if evt['Rec/Header']:
      print 'write event', evt['Rec/Header']
      appMgr.algorithm( 'InputCopyStream' ).execute()
    else:
      rv = 1
      logging.warning("no event header ( %d )" % pos)
      if stop_on_error:
        return {"success": False, "position": pos}
  return rv


def fetch_event( pfn_request_list, output_file, lfn_request_list=None, lfn_to_pfn_map=None ):
  """Downloads events into output_file.

  Args:
    pfn_request_list: list of events to dowload in format
      [[<PFN1>, [<list of positions>]], [<PFN2>, ....
    output_file: output file name
    lfn_request_list: list of events to dowload in format
      [[<LFN1>, [<list of positions>]], [<LFN2>, ....
      if used must also specify lfn_to_pfn_map
    lfn_to_pfn_map: dict, mapping from LFN to PFNs with keys being normalised LFNs
      and values being lists of PFNs. If not specified lfn_request_list is ignored.
  """
  rv = 0
  InputCopyStream().Output = "DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='REC' " % output_file
  appConf = ApplicationMgr( OutputLevel = INFO, AppName = 'fetch_event' )
  appConf.OutStream = [InputCopyStream()]
  appMgr = GaudiPython.AppMgr()
  sel = appMgr.evtsel()
  evt = appMgr.evtsvc()
  appMgr.algorithm( 'InputCopyStream' ).Enable = False

  if pfn_request_list:
    for PFN, positions in pfn_request_list:
      rv += process_pfn( sel, evt, appMgr, PFN, positions, stop_on_error = False )
  if lfn_request_list and lfn_to_pfn_map:
    for LFN, positions in lfn_request_list:
      successfuly_processed_lfn = False
      for PFN in lfn_to_pfn_map[normalize_lfns(LFN)]:
        process_result = process_pfn( sel, evt, appMgr, PFN, positions, stop_on_error = True )
        if process_result == 0:
          successfuly_processed_lfn = True
          break
        elif isinstance(process_result, dict):
          positions = filter(lambda x: x >= process_result['position'],
                             positions)
          logging.warning( "PFN %s failed on position %d" % (PFN, process_result['position']) )
        else:
          raise RuntimeError("Incorrect retun value from process_pfn.")
      if not successfuly_processed_lfn:
        logging.error( "Failed to process LFN %s" % LFN )
        rv += 1
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
  assert request.pfn_req_list is not None or request.lfn_to_pfn_map is not None, \
    "empty PFN request list and no LFN->PFN map"
  return fetch_event( request.pfn_req_list, output_file,
                      lfn_request_list = request.req_list,
                      lfn_to_pfn_map = request.lfn_to_pfn_map)


if __name__ == '__main__':
  sys.exit(main())

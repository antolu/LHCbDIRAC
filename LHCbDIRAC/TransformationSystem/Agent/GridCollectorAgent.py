""" GridCollectorAgent is for
    <add doc>
"""

__RCSID__ = "$Id$"

import os
import sys
import re
import smtplib
import subprocess as subp

from random import randint
from email.mime.text import MIMEText

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

BASE_DIR = "/var/www/grid-collector"
sys.path.append( BASE_DIR + "/src" )
from config import STATUS_RUNNING, STATUS_NEW, STATUS_DONE, STATUS_FAIL, \
    DOWNLOADS_CACHE_DIR, DOWNLOADS_REQUEST_DIR
from request import Request
from request import normalize_lfns

MAILFROM = 'EventIndex Grid Collector <dirac@eindex.cern.ch>'
MAILHOST = 'localhost'
DASHBOARD_LINK = 'https://eindex.cern.ch/dashboard'

# # SE: RAL-DST, CERN-DST-EOS, RAL-ARCHIVE, PIC-DST, SARA_M-DST

SE_WEIGHTS = {'CERN.*': 10,
              '.*-ARCHIVE':-1}
SE_DEFAULT_WEIGHT = 1


def sort_se_weighted( storage_elements, black_list = [], cut_negative = True ):
  result = {}
  for se in storage_elements:
    if se in black_list:
      continue
    for se_re in SE_WEIGHTS.keys():
      if re.search( se_re, se ) is not None:
        result[se] = SE_WEIGHTS[se_re]
        break
    if se not in result:
      result[se] = SE_DEFAULT_WEIGHT
    if cut_negative and result[se] < 0:
      del result[se]
  sorted_se = sorted( result, key = result.get, reverse = True )
  return sorted_se


def get_lfn2pfn_map( rm, lfns, se_black_list = [], get_single = True ):
  lfns = normalize_lfns( lfns )
  res = rm.getCatalogReplicas( lfns )
  assert res['OK'] is True, "error getting catalog replicas"
  lfn2pfn_map = {}
  for lfn, se2lfn_map in res['Value']['Successful'].iteritems():
    lfn2pfn_map[lfn] = []
    for se in sort_se_weighted( se2lfn_map.keys(), se_black_list, cut_negative = True ):
      res_url = rm.getReplicaAccessUrl( [lfn, ], se )
      if res_url['OK']:
        if lfn in res_url['Value']['Successful']:
          pfn = res_url['Value']['Successful'][lfn]
          lfn2pfn_map[lfn].append( pfn )
          if get_single:
            break
    assert len( lfn2pfn_map[lfn] ) > 0, "cannot match lfn to pfns"
  return lfn2pfn_map

def notify_email( request ):
  if request.email is None:
    return
  subject = "EventIndex grid-collector report (%s)" % request.status
  if request.status == STATUS_DONE:
    body = """\
Your download request '%s' has completed successfully.
Please, proceed to dashboard for download (%s) or
download results directly:

%s

--
Faithfully Yours,
EventIndex
""" % ( request.id, DASHBOARD_LINK, request.get_url() )
  else:
    body = """\
There was an error proceeding your request '%s'.
Please contact EventIndex support.
Request processing details: %s

--
Faithfully Yours,
EventIndex
""" % ( request.id, request.details )
  try:
    msg = MIMEText( body )
    msg['To'] = request.email
    msg['From'] = MAILFROM
    msg['Subject'] = subject

    smtpObj = smtplib.SMTP( MAILHOST )
    smtpObj.sendmail( MAILFROM, [request.email], msg.as_string() )
    gLogger.info( "Successfully sent email to %s" % request.email )
  except smtplib.SMTPException, e:
    gLogger.error( "Error: unable to send email to '%s' (%s)" % ( request.email, str( e ) ) )

class GridCollectorAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )
    self.dataManager = None

  def initialize( self ):
    """ agent initialization
    """
    self.dataManager = DataManager()
    self.am_setOption( 'shifterProxy', 'DataManager' )
    return S_OK()

  def execute( self ):
    """ execution method.
    """
    r = self.get_next_request()
    if r is None:
      return S_OK()
    gLogger.info( "found request: %s (%d lfns)" % ( r.req_file, len( r.req_list ) ) )
    result = self.download( r.req_file, "%s/%s.root" % ( DOWNLOADS_CACHE_DIR, r.id ) )
    if result == 0:
      return S_OK()
    else:
      return S_ERROR()

  def get_next_request( self ):
    new_files = [f for f in os.listdir( DOWNLOADS_REQUEST_DIR ) if f.endswith( STATUS_NEW )]
    request = None
    if len( new_files ) > 0:
      pos = randint( 0, len( new_files ) - 1 )
      # TODO: check if it exists yet and rename first
      request = Request( req_file = DOWNLOADS_REQUEST_DIR + "/" + new_files[pos] )
      request.change_status( STATUS_RUNNING )
    return request

  def lfn2pfn_update( self, request ):
    PFN_map = get_lfn2pfn_map( self.dataManager, [r[0] for r in request.req_list] )
    request.lfn2pfn( PFN_map )
    request.save()

  def download( self, req_file, out_file ):
    rv = 1
    try:
      request = Request( req_file = req_file )
      self.lfn2pfn_update( request )
      p = subp.Popen( ["%s/run_fetch.sh" % BASE_DIR, req_file, out_file], stdout = subp.PIPE, stderr = subp.PIPE )
      stdout, stderr = p.communicate()
      gLogger.info( stdout )
      if len( stderr ) > 0:
        gLogger.error( stderr )
      if os.path.exists( out_file ):
        request.change_status( STATUS_DONE, "OK" )
        rv = 0
      else:
        request.change_status( STATUS_FAIL, "error creating file '%s'" % out_file )
    except Exception, e:
      gLogger.error( "Exception: " + str( e ) )
      request.change_status( STATUS_FAIL, "Grid-collector exception occurred: " + str( e ) )
    notify_email( request )
    return rv

  def fake_download( self, request ):
    outfile = "%s/%s.root" % ( DOWNLOADS_CACHE_DIR, request.id )
    lfns = [lfn for lfn, _pos in request.req_list]
    with open( outfile, 'w' ) as fh:
      fh.write( '\n'.join( lfns ) )
    gLogger.info( "created file: %s" % outfile )
    return S_OK()

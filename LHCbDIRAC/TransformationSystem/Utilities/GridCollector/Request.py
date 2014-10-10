""" Download file from Grid request class 
"""

__RCSID__ = "$Id$"

import os
import re
import uuid
import cPickle
from datetime import datetime
from pprint import pprint
#from LHCbDIRAC.TransformationSystem.Utilities.GridCollector.Config import DOWNLOADS_REQUEST_DIR, DOWNLOADS_BASE_URL, \
from Config import DOWNLOADS_REQUEST_DIR, DOWNLOADS_BASE_URL, \
                                                                          STATUS_NEW, STATUS_INVALID, TYPE_ROOT, TYPE_SE


def normalize_lfns( lfns ):
  if type( lfns ) == str:
    return lfns.replace( "LFN:", "" )
  else:
    return [s.replace( "LFN:", "" ) for s in lfns]


class Request( object ):
  basedir = DOWNLOADS_REQUEST_DIR

  def __init__( self, fId = None, req_list = None, status = STATUS_NEW, req_file = None, email = None, fType = None ):
    if req_file is None:
      self.id = fId if fId is not None else str( uuid.uuid1() )
      self.status = status
      self.req_list = req_list
      self.age = None
      self.details = ""
      self.req_file = self.filename()
      self.email = email
      self.type = fType
      self.pfn_req_list = None
      if not self.validate():
        self.id = -1
    else:
      if not os.path.exists( req_file ):
        self.status = STATUS_INVALID
        self.details = "file not found: %s" % req_file
      else:
        self.req_file = req_file
        self.load()

  def validate( self ):
    rv = True
    if self.email is not None and re.match( "[^@]+@[^@]+\.[^@]+", self.email ) is None:
      self.details = "invalid email: %s" % self.email
      rv = False
    if self.type is not None and self.type != TYPE_ROOT and self.type != TYPE_SE:
      self.details += "\ninvalid type: %s" % self.type
      rv = False
    if self.req_list is not None and type( self.req_list ) != list and type( self.req_list ) != tuple:
      self.details = "\ninvalid request: %s" % self.req_list
      rv = False
    elif self.req_list is not None:
      try:
        for lfn, pos in self.req_list:
          if lfn is None or pos is None or len( pos ) == 0:
            self.details += "\ninvalid req record: %s" % lfn
            rv = False
      except Exception, e:
        self.details += "\n%s" % e
        rv = False
    if not rv:
      print self.details
    return rv

  def filename( self ):
    assert self.id != -1
    return os.path.join( self.basedir, "%s.%s" % ( self.id, self.status ) )

  def get_age( self, filename ):
    age = -1
    if os.path.exists( filename ):
      created_at = os.stat( filename ).st_ctime
      age = ( datetime.now() - datetime.fromtimestamp( created_at ) ).seconds
    return age

  def _guess_status( self, ffname ):
    assert os.path.exists( ffname )
    f, s = os.path.splitext( ffname )
    self.id = os.path.basename( f )
    self.status = s[1:]
    self.req_file = ffname

  def find_by_id( self ):
    fnames = [f for f in os.listdir( self.basedir ) if f.startswith( self.id )]
    if len( fnames ) == 1:
      self._guess_status( self.basedir + "/" + fnames[0] )
      self.load()
    else:
      self.status = STATUS_INVALID
      self.details = "file not found: %s" % self.req_file
    return self.status

  def lfn2pfn( self, PFN_map ):
    self.pfn_req_list = [[PFN_map[normalize_lfns( LFN )][0], pos] for ( LFN, pos ) in self.req_list]  # TODO: remove indexing [0], make map one-2-one
    pprint( self.pfn_req_list )

  def load( self ):
    assert os.path.exists( self.req_file ), "load: file not found: %s" % self.req_file
    fname, ext = os.path.splitext( self.req_file )
    self.id = os.path.basename( fname )
    self.status = ext.strip( '.' )
    self.age = self.get_age( self.req_file )
    with open( self.req_file ) as fh:
      req_hash = cPickle.load( fh )
      self.req_list = req_hash['request']
      self.details = req_hash['details']
      self.email = req_hash['email']
      self.type = req_hash['type']
      self.pfn_req_list = None
      if 'pfn_request' in req_hash:
        self.pfn_req_list = req_hash['pfn_request']

  def save( self ):
    if not os.path.exists( self.basedir ):
      os.makedirs( self.basedir, mode = 0755 )
    with open( self.req_file, 'w' ) as request_file:
      req_hash = {'request': self.req_list,
                  'pfn_request': self.pfn_req_list,
                  'details': self.details,
                  'email': self.email,
                  'type': self.type}
      cPickle.dump( req_hash, request_file )

  def change_status( self, new_status, details = "" ):
    # fname = self.filename()
    assert os.path.exists( self.req_file ), "change_status: file not found: %s" % self.req_file
    f, _ext = os.path.splitext( self.req_file )
    new_filename = "%s.%s" % ( f, new_status )
    os.renames( self.req_file, new_filename )  # just to save ctime
    self.req_file = new_filename
    self.status = new_status
    if details != "":
      self.details = details
    self.save()
    return self.status

  def get_url( self ):
    url = "%s/%s.root" % ( DOWNLOADS_BASE_URL, self.id )
    return url

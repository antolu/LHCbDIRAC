########################################################################
# $Id: $
########################################################################

import json
import datetime


__RCSID__ = " "

class JSONDateTimeEncoder( json.JSONEncoder ):
    
  def default( self, obj ):
    if isinstance( obj, ( datetime.date, datetime.datetime ) ):
      return '?*' + obj.isoformat()
    else:
      return json.JSONEncoder.default( self, obj )

def datetime_decoder( d ):
  if isinstance( d, list ):
    pairs = enumerate( d )
  elif isinstance( d, dict ):
    pairs = d.items()
  result = []
  for k, v in pairs:
    if isinstance( v, basestring ) and v.startswith( '?*' ):
      try:
        # The %f format code is only supported in Python >= 2.6.
        # For Python <= 2.5 strip off microseconds
        # v = datetime.datetime.strptime(v.rsplit('.', 1)[0],
        #     '%Y-%m-%dT%H:%M:%S')
        v = datetime.datetime.strptime( v, '?*%Y-%m-%dT%H:%M:%S' )
      except ValueError:
        try:
          v = datetime.datetime.strptime( v, '?*%Y-%m-%dT%H:%M:%S.%f' )
        except ValueError:
            try:
              v = datetime.datetime.strptime( v, '?*%Y-%m-%d' ).date()
            except ValueError:
              pass
    elif isinstance( v, ( dict, list ) ):
      v = datetime_decoder( v )
    result.append( ( k, v ) )
  if isinstance( d, list ):
    return [x[1] for x in result]
  elif isinstance( d, dict ):
    return dict( result )

def dumps( obj ):
  return json.dumps( obj, cls = JSONDateTimeEncoder, encoding = 'utf-8' )

def loads( obj ):
  return json.loads( obj, encoding = 'utf-8', object_hook = datetime_decoder )

def load( fd ):
  return json.load( fd, object_hook = datetime_decoder, encoding = 'utf-8' )

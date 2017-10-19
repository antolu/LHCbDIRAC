""" JSON encoder - slighltly modified version for LHCbDIRAC purposes
"""

import json
import datetime


__RCSID__ = "$Id$"

class JSONDateTimeEncoder( json.JSONEncoder ):
  """ encoder of datetime objects
  """

  def default( self, obj ): #pylint: disable=method-hidden
    if isinstance( obj, datetime.datetime ):
      return {'dt': [obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second]}
    elif isinstance( obj, datetime.date ):
      return {'d': [obj.year, obj.month, obj.day] }
    elif isinstance( obj, datetime.time ):
      return {'t': [obj.hour, obj.minute, obj.second] }

    return super( JSONDateTimeEncoder, self ).default( obj )

class JSONDateTimeDecoder( json.JSONDecoder ):
  """ decoder of datetime objects
  """

  def __init__( self, *args, **kwargs ):
    json.JSONDecoder.__init__( self, object_hook = self.object_hook, *args, **kwargs )

  def object_hook( self, obj ): #pylint: disable=method-hidden
    if 'dt' in obj:
      obj = datetime.datetime( obj['dt'][0], obj['dt'][1], obj['dt'][2], obj['dt'][3], obj['dt'][4], obj['dt'][5] )
    elif 'd' in obj:
      obj = datetime.date( obj['d'][0], obj['d'][1], obj['d'][2] )
    elif 't' in obj:
      obj = datetime.time( obj['t'][0], obj['t'][1], obj['t'][2] )

    return obj

def dumps( obj ):
  return json.dumps( obj, cls = JSONDateTimeEncoder, encoding = 'utf-8' )

def loads( obj ):
  return json.loads( obj, cls = JSONDateTimeDecoder, encoding = 'utf-8' )

def load( fd ):
  return json.load( fd, cls = JSONDateTimeDecoder, encoding = 'utf-8' )

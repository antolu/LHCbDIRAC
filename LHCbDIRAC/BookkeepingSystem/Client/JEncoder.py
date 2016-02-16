########################################################################
# $Id: $
########################################################################

import json
import datetime


__RCSID__ = " "

class JSONDateTimeEncoder( json.JSONEncoder ):
    
  def default( self, obj ):
    if isinstance( obj, datetime.datetime ):
      return {'dt': [obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second]}
    elif isinstance( obj, datetime.date ):
      return {'d': [obj.year, obj.month, obj.day] }
    elif isinstance( obj, datetime.time ):
      return {'t': [obj.hour, obj.minute, obj.second] }
    
    return super( json.JSONEncoder, self ).default( obj )

class JSONDateTimeDecoder( json.JSONDecoder ):

  def __init__( self, *args, **kwargs ):
    json.JSONDecoder.__init__( self, object_hook = self.object_hook, *args, **kwargs )
  
  def object_hook( self, d ):
    if 'dt' in d:
      d = datetime.datetime( d['dt'][0], d['dt'][1], d['dt'][2], d['dt'][3], d['dt'][4], d['dt'][5] ) 
    elif 'd' in d:
      d = datetime.date( d['d'][0], d['d'][1], d['d'][2] )
    elif 't' in d:
      d = datetime.time( d['t'][0], d['t'][1], d['t'][2] )
    
    return d   

def dumps( obj ):
  return json.dumps( obj, cls = JSONDateTimeEncoder, encoding = 'utf-8' )

def loads( obj ):
  return json.loads( obj, cls = JSONDateTimeDecoder, encoding = 'utf-8' )

def load( fd ):
  return json.load( fd, cls = JSONDateTimeDecoder, encoding = 'utf-8' )

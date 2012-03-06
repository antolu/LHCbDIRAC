################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"

import time

from xml.dom.minidom import Document
from datetime        import datetime 

from DIRAC           import gLogger
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

# Not best solution, but avoids a big number of connections to the DB oppened.
rmc = ResourceManagementClient()

def writeXml( task, taskResult ):  

  # This 3 keys must exist
  xmlList  = taskResult.get( 'xmlList' )
  filename = taskResult.get( 'filename' )
  config   = taskResult.get( 'config' )
  
  workdir  = config.get( 'workdir' )
  testName = config.get( 'testName' )

  path = '%s/%s' % ( workdir, testName )
    
  d = Document()
  d = _writeXml( d, d, xmlList )
  
  try:       
    file = open( '%s/%s' % ( path, filename ), 'w' )
    file.write( d.toxml() )
    file.close()
    gLogger.info( '%s: %s written' % ( config[ 'testName' ], filename ) )
  except:
    gLogger.error( '%s: %s NOT written' % ( config[ 'testName' ], filename ) )
    return False     
    
  return d.toxml()

def writeSLSXml( task, taskResult ):  

  try:

    # This 2 keys must exist
    xmlDict  = taskResult.get( 'xmlDict' )
    config   = taskResult.get( 'config' )
    
    filename = '%s.xml' % xmlDict[ 'id' ]
    target   = xmlDict[ 'target' ]
   
    workdir  = config.get( 'workdir' )
    testName = config.get( 'testName' )
    path     = '%s/%s' % ( workdir, testName )  
    
    
    xmlList  = []
    # mandatory tags 
    xmlList.append( { 'tag' : 'id',                'nodes' : xmlDict[ 'id' ] } )
    xmlList.append( { 'tag' : 'availability',      'nodes' : xmlDict[ 'availability' ] } )
    xmlList.append( { 'tag' : 'availabilityinfo',  'nodes' : xmlDict[ 'availabilityinfo' ] } )

    xmlList.append( { 'tag' : 'availabilitydesc',  'nodes' : config[ 'availabilitydesc' ] } )
    xmlList.append( { 'tag' : 'refreshperiod'    , 'nodes' : config[ 'refreshperiod' ] } )
    xmlList.append( { 'tag' : 'validityduration' , 'nodes' : config[ 'validityduration' ] } )   
  
    xmlList.append( { 'tag' : 'timestamp' ,        'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) } )
  
#    xmlThresholds = []
#    if config.has_key( 'thresholds' ):
#      thrh = config[ 'thresholds' ]
#      xmlThresholds.append( { 'tag': 'threshold', 'attrs' : [ ( 'level', 'available' ) ], 'nodes' : thrh[ 'available' ] } )
#      xmlThresholds.append( { 'tag': 'threshold', 'attrs' : [ ( 'level', 'affected' ) ],  'nodes' : thrh[ 'affected' ] } )
#      xmlThresholds.append( { 'tag': 'threshold', 'attrs' : [ ( 'level', 'degraded' ) ],  'nodes' : thrh[ 'degraded' ] } )
#      xmlList.append( { 'tag' : 'availabilitythresholds', 'nodes' : xmlThresholds })
  
    def processData( dItem ):
      dataDict = { 'tag' : dItem[0] }
      attrs = []
      if dItem[1] is not None:
        attrs.append( ( 'name', dItem[ 1 ] ) )
      if dItem[2] is not None:
        attrs.append( ( 'desc', dItem[ 2 ] ) )
      if attrs:
        dataDict[ 'attrs' ] = attrs  
      dataDict[ 'nodes' ] = dItem[ 3 ]
  
      return dataDict
  
    if xmlDict.has_key( 'data' ):
    
      dataList = []
      data     = xmlDict[ 'data' ]
    
      for dItem in data:
    
        if not isinstance( dItem, dict ):
          dataList.append( processData( dItem ) )
        else:
          name = dItem.keys()[ 0 ]
          groupDict = { 'tag' : 'grp', 'attrs' : [ ( 'name', name ) ] }
          groupList = []
          for v in dItem[ name ]:
            groupList.append( processData( v ) )
          if groupList:
            groupDict[ 'nodes' ] = groupList               
          dataList.append( groupDict )
        
      xmlList.append( { 'tag' : 'data', 'nodes' : dataList } )    
      
    XML_STUB = [ { 
                'tag'   : 'serviceupdate',
                'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                            ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                            ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                          ],
                'nodes' : xmlList }
                ]
    
    
    
    res = rmc.addOrModifySLSTest( testName, target, xmlDict[ 'availability' ], 
                                  xmlDict[ 'metric' ], xmlDict[ 'availabilityinfo' ], 
                                  datetime.utcnow() )
    if not res[ 'OK' ]:
      gLogger.exception( res[ 'Message' ] )
    
  except Exception, e:
    gLogger.exception( e )
    return False  
    
  d = Document()
  d = _writeXml( d, d, XML_STUB )
  
  try:       
    file = open( '%s/%s' % ( path, filename ), 'w' )
    file.write( d.toxml() )
    file.close()
    gLogger.info( '%s: %s written' % ( config[ 'testName' ], filename ) )
  except:
    gLogger.error( '%s: %s NOT written' % ( config[ 'testName' ], filename ) )
    return False     
    
  return d.toxml()

def _writeXml( doc, topElement, elementList ):

  try:

    if elementList is None:
      return topElement
    
    elif not isinstance( elementList, list ):
      tn = doc.createTextNode( str( elementList ) )
      topElement.appendChild( tn )
      return topElement

    for d in elementList:
      
      el = doc.createElement( d[ 'tag' ] )
      
      for attr in d.get( 'attrs', [] ):
        el.setAttribute( attr[0], attr[1] )
        
      el = _writeXml( doc, el, d.get( 'nodes', None ) )
      topElement.appendChild( el )
    
    return topElement
  
  except Exception, e:
    gLogger.exception( e ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
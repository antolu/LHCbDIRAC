################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id:  $"

from xml.dom.minidom                      import Document

#def writeXml( xmlList, fileName, useStub = True, path = None ):
def writeXml( task, taskResult ):  

  print taskResult

  # This 3 keys must exist
  xmlList  = taskResult.get( 'xmlList' )
  filename = taskResult.get( 'filename' )
  config   = taskResult.get( 'config' )
  
  # The following are optional
  useStub  = taskResult.get( 'useStub', True )
  
  path    = config.get( 'path', None ) 
  workdir = config.get( 'workdir' )

  if path is None:
    path = workdir
    
  XML_STUB = [ { 
                'tag'   : 'serviceupdate',
                'attrs' : [ ( 'xmlns', 'http://sls.cern.ch/SLS/XML/update' ),
                            ( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' ),
                            ( 'xsi:schemaLocation', 'http://sls.cern.ch/SLS/XML/update http://sls.cern.ch/SLS/XML/update.xsd' )
                          ],
                'nodes' : xmlList }
              ]
    
    
  d = Document()
  d = _writeXml( d, d, ( XML_STUB and useStub ) or xmlList )
    
#  gLogger.info( d.toxml() )
  
  print path
  print filename
    
#  file = open( '%s/%s' % ( path, filename ), 'w' )
#  try:
#    file.write( d.toxml() )
#  finally:  
#    file.close()

  print d.toxml()
  return d.toxml()

def _writeXml( doc, topElement, elementList ):

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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
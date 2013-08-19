"""

  LHCbDIRAC changelog

  @author: mario.ubeda.garcia@cern.ch

"""


import os
import sys

from xml.dom import minidom


def parseSvnXml( filePath ):

  dom = minidom.parse( open( filePath, 'r' ) )

  changelog = {}

  for logEntry in dom.getElementsByTagName( 'logentry' ):
  
    revision = int( logEntry.getAttribute( 'revision' ) )
    author   = __getNode( logEntry, 'author' )
    msg      = __getNode( logEntry, 'msg' )
    paths    = __getPaths( logEntry )
  
    changelog[ revision ] = {
                           'author' : author,
                           'msg'    : msg,
                           'paths'  : paths
                           }
  return changelog


def __getNode( parent, nodeName ):
  
  return str( parent.getElementsByTagName( nodeName )[ 0 ].firstChild.data )  
  
def __getPaths( parent ):
  
  paths     = []
  pathsDom  = parent.getElementsByTagName( 'path' )
  
  for pathDom in pathsDom:
    
    paths.append( ( str( pathDom.firstChild.data), str( pathDom.getAttribute( 'action' ) ) ) )
  
  
  return paths

def generateChangeLog( xmlFile, logDirPath, version, prevRevision, thisRevision ):


  versionDir = os.path.join( logDirPath, version )

  try:
    os.mkdir( versionDir )
  except:
    pass     

  parsedXML = parseSvnXml( xmlFile )

  revisions = parsedXML.keys()
  revisions.sort()
  revisions.reverse()

  __writeIndex( versionDir, parsedXML, revisions, version, prevRevision, thisRevision )
  
  
  changelog = os.path.join( versionDir, 'changelog.html' )
  
  for rev in revisions:
    
    commitFilePath = os.path.join( versionDir, '%s.html' % rev )
    
    __writeLog( commitFilePath, parsedXML, rev )
    __writeLog( changelog     , parsedXML, rev )
    
  

def __writeIndex( logDir, parsedXML, sortedRevisions, version, prevRevision, thisRevision ):

  indexPath = os.path.join( logDir, 'index.html' )

  indexFile = open( indexPath, 'w' )
  indexFile.write( '<html><body>' )
  indexFile.write( '<h1>ChangeLog %s </h1>\n' % version )
  indexFile.write( '<h3>changes between %s:%s</h3>\n' % ( prevRevision, thisRevision ) )
  
  files   = set()
  authors = set()
  for revDict in parsedXML.itervalues():
    files.update( [ p[ 0 ] for p in revDict[ 'paths' ] ] ) 
    authors.update( revDict[ 'author' ] ) 
  
  print authors
  
  indexFile.write( '<p>%s (different) files changed in %s commits by %s authors ( <a href="changelog.html">full log</a> )</p>' % ( len( files ), len( parsedXML ), len( authors ) ) )
    
  indexFile.write( '<ul style="list-style: none;">\n' )
  
  for rev in sortedRevisions:
    
    if '#NOLOG' in parsedXML[ rev ][ 'msg' ]:
      continue
    
    indexFile.write( '<li><a href="%s.html">%s</a></li>\n' % ( rev, parsedXML[ rev ][ 'msg' ] ) )
     
  indexFile.write( '</ul>\n' )

  indexFile.write( '</body></html>' )
  indexFile.close() 


def __writeLog( logFilePath, parsedXML, revision ):
  
    svnURL = "http://svnweb.cern.ch/world/wsvn/dirac/%s?op=diff&rev=%s"
  
    logFile = open( logFilePath, 'a' )
    
    logFile.write( '<div id="%s">\n' % revision )
    logFile.write( '<div style="border:solid 2px;padding-left:10px;">')
    logFile.write( '<p>%s<br/>' % parsedXML[ revision ][ 'msg' ] )
    logFile.write( 'Commit %s by %s</p>' % ( revision, parsedXML[ revision ][ 'author' ] ) )
    logFile.write( '</div>')
    
    logFile.write( '<ul style="list-style: none;">\n' )
    for path in parsedXML[ revision ][ 'paths' ]:
      
      line = '<li>%s <a href="%s">(diff)</a></li>' % ( path[ 0 ], svnURL % ( path[0], revision ) )
      
      if path[ 1 ] == 'D':
        line = '<span style="color:red;">' + line + '</span>'  
      elif path[ 1 ] == 'A':
        line = '<span style="color:green;">' + line + '</span>'  
      
      logFile.write( line )
      
    logFile.write( '</ul>\n' )
    logFile.write( '</div><br/>\n' )

    logFile.close()    


if __name__ == "__main__":
  
  
  generateChangeLog( *sys.argv[1:] )


#...............................................................................
#EOF

"""
Collection of utilities function
"""

def makeRunList( runInput ):
  """ makeRunList return a list of runs starting from a string.
      Example:
      makeRunList("1234:1236,12340,12342,1520:1522") --> ['1234','1235','1236','12340','12342','1520','1521','1522']
  """

  from DIRAC import S_OK, S_ERROR

  try:
    # remove blank spaces
    l = ''.join( runInput.split() )
    i = l.split( "," )
    runList = []
    for part in i:
      if part.find( ':' ):
        pp = part.split( ":" )
        for p in xrange( int( pp[0] ), int( pp[len( pp ) - 1] ) + 1 ):
          runList.append( str( p ) )
      else:
        runList.append( str( part ) )
    return S_OK( runList )
  except Exception:
    return S_ERROR( "Could not parse runList " )

#############################################################################

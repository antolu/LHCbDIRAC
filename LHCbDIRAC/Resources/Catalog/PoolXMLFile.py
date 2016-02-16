""" Interacts with pool xml catalog
"""

from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.Resources.Catalog.PoolXMLFile import _getPoolCatalogs

def getOutputType( outputs, inputs, directory = '' ):
  """ This function searches the directory for POOL XML catalog files and extracts the type of the pfn.

      If not found, inherits from the type of the inputs
  """

  if not type( outputs ) == type( [] ):
    outputs = [outputs]

  catalog = PoolXMLCatalog( _getPoolCatalogs( directory ) )

  #inputs - by lfn
  generatedIn = False
  typeFileIn = []
  for fname in inputs:
    try:
      tFileIn = str( catalog.getTypeByPfn( str( catalog.getPfnsByLfn( fname )['Replicas'].values()[0] ) ) )
    except KeyError:
      tFileIn = None
    if not tFileIn:
      generatedIn = True
    else:
      typeFileIn.append( tFileIn )

  if generatedIn and inputs:
    raise ValueError( 'Could not find Type for inputs' )

  #outputs - by pfn
  pfnTypesOut = {}
  for fname in outputs:
    tFileOut = str( catalog.getTypeByPfn( fname ) )
    if not tFileOut:
      if typeFileIn:
        tFileOut = typeFileIn[0]
      else:
        tFileOut = 'ROOT'
    pfnTypesOut[fname] = tFileOut

  return pfnTypesOut

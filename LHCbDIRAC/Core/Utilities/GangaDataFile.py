""" GangaDataFile is a utility to create a Data file, to be used by ganga.

    Given input files, it will create something like:
    
    from GaudiConf import IOExtension
    IOExtension("ROOT").inputFiles([
        "LFN:foo",
        "LFN:bar"
    ], clear=True)

    from Gaudi.Configuration import FileCatalog
    FileCatalog().Catalogs = ["xmlcatalog_file:pool_xml_catalog.xml"]
"""

__RCSID__ = "$Id$"

import os, fnmatch
from DIRAC import gLogger

class GangaDataFile( object ):
  """ Creates ganga data file
  """

  def __init__( self, fileName = 'data.py', xmlcatalog_file = 'pool_xml_catalog.xml', log = None ):
    """ initialize
    """
    if not log:
      self.log = gLogger.getSubLogger( 'GangaDataFile' )
    else:
      self.log = log

    self.fileName = fileName
    self.xmlcatalog_file = xmlcatalog_file

    try:
      os.remove( self.fileName )
    except OSError:
      pass

    self.log.info( 'Creating Ganga data file %s from scratch' % self.fileName )
    fopen = open( self.fileName, 'w' )
    fopen.close()

  ################################################################################

  def generateDataFile( self, lfns, persistency = None,
                        TSDefaultStr = "TYP='POOL_ROOTTREE' OPT='READ'",
                        TSLookupMap = {'*.raw':"SVC='LHCb::MDFSelector'",
                                       '*.RAW':"SVC='LHCb::MDFSelector'",
                                       '*.mdf':"SVC='LHCb::MDFSelector'",
                                       '*.MDF':"SVC='LHCb::MDFSelector'"} ):
    """ generate the data file
    """

    if ( type( lfns ) is not type( [] ) ):
      self.log.error( 'Was expecting a list' )
      raise TypeError( 'Expected List' )
    if not len( lfns ):
      self.log.warn( 'No file generated: was expecting a non-empty list' )
      raise ValueError( 'list empty' )

    script = ''
    try:
      persistency = persistency.upper()
    except AttributeError:
      pass

    if persistency == 'ROOT':
      script = self.__inputFileStringBuilder( len( lfns ) ) % tuple( lfns )
    elif persistency == 'POOL':
      script = self.__legacyInputFileStringBuilder( len( lfns ),
                                                    persistency ) % ( tuple( lfns ) + self.__buildTypeSelectorTuple( lfns,
                                                                                                                     TSDefaultStr,
                                                                                                                     TSLookupMap ) )
    else:
      script = self.__legacyInputFileStringBuilder( len( lfns ),
                                                    None ) % ( tuple( lfns ) + self.__buildTypeSelectorTuple( lfns,
                                                                                                              TSDefaultStr,
                                                                                                              TSLookupMap ) )

    f = open( self.fileName, 'w' )
    f.write( script )
    f.close()

    self.log.info( 'Created Ganga data file %s' % self.fileName )

    return script

  ################################################################################

  def __inputFileStringBuilder( self, entries ):
    """ ROOT extension
    """

    script = '\n#new method'
    script += '\nfrom GaudiConf import IOExtension'
    script += '\nIOExtension("ROOT").inputFiles(['
    script += ( '\n    "LFN:%s",' * entries )[:-1]
    script += '\n], clear=True)'
    script += '\n'
    script += '\nfrom Gaudi.Configuration import FileCatalog'
    script += '\nFileCatalog().Catalogs = ["xmlcatalog_file:%s"]' % self.xmlcatalog_file
    return script

  ################################################################################

  def __legacyInputFileStringBuilder( self, entries, persistency = None ):
    """ POOL extension
    """

    script = '\ntry:'
    script += '\n    #new method'
    script += '\n    from GaudiConf import IOExtension'
    if persistency:
      script += '\n    IOExtension("%s").inputFiles([' % persistency
    else:
      script += '\n    IOExtension().inputFiles(['
    script += ( '\n        \"LFN:%s\",' * entries )[:-1]
    script += '\n    ], clear=True)'
    script += '\nexcept ImportError:'
    script += '\n    #Use previous method'
    script += '\n    from Gaudi.Configuration import EventSelector'
    script += '\n    EventSelector().Input=['
    script += ( '\n        "DATAFILE=\'LFN:%s\' %s",' * entries )[:-1]
    script += '\n    ]'

    script += '\n'
    script += '\nfrom Gaudi.Configuration import FileCatalog'
    script += '\nFileCatalog().Catalogs = ["xmlcatalog_file:%s"]' % self.xmlcatalog_file

    return script

  ################################################################################

  def __typeSelectorString( self, filename, defaultStr = "TYP='POOL_ROOTTREE' OPT='READ'",
                            lookupMap = {'*.raw':"SVC='LHCb::MDFSelector'",
                                         '*.RAW':"SVC='LHCb::MDFSelector'",
                                         '*.mdf':"SVC='LHCb::MDFSelector'",
                                         '*.MDF':"SVC='LHCb::MDFSelector'"} ):
    """ helper function 
    """

    for key, val in lookupMap.iteritems():
      if fnmatch.fnmatch( filename, key ):
        return val

    return defaultStr

  ################################################################################

  def __buildTypeSelectorTuple( self, lfns, TSDefaultStr = "TYP='POOL_ROOTTREE' OPT='READ'",
                                TSLookupMap = {'*.raw':"SVC='LHCb::MDFSelector'",
                                               '*.RAW':"SVC='LHCb::MDFSelector'",
                                               '*.mdf':"SVC='LHCb::MDFSelector'",
                                               '*.MDF':"SVC='LHCb::MDFSelector'"} ):
    """ helper function 
    """

    r = []
    for i in range( len( lfns ) ):
      r.extend( [lfns[i], self.__typeSelectorString( lfns[i], TSDefaultStr, TSLookupMap )] )
    return tuple( r )

################################################################################

"""
 LHCb Bookkeeping database client
"""

########################################################################
# $Id$
########################################################################

from LHCbDIRAC.BookkeepingSystem.Client.BaseESClient                        import BaseESClient
from LHCbDIRAC.BookkeepingSystem.Client.LHCbBookkeepingManager              import LHCbBookkeepingManager

__RCSID__ = "$Id$"

#############################################################################
class LHCB_BKKDBClient( BaseESClient ):
  """Client which used to browse the Entities"""
  #############################################################################
  def __init__( self, rpcClinet = None, web = False ):
    """Initialize the basic class"""
    BaseESClient.__init__( self, LHCbBookkeepingManager( rpcClinet, web ), '/' )
  #############################################################################
  def get( self, path = "" ):
    """get path"""
    return self.getManager().get( path )

  #############################################################################
  def help( self ):
    """help function"""
    return self.getManager().help()

  #############################################################################
  def getPossibleParameters( self ):
    """available trees"""
    return self.getManager().getPossibleParameters()

  #############################################################################
  def setParameter( self, name ):
    """tree used"""
    return self.getManager().setParameter( name )

  #############################################################################
  def getLogicalFiles( self ):
    """lfns"""
    return self.getManager().getLogicalFiles()

  #############################################################################
  def getFilesPFN( self ):
    """PFNS"""
    return self.getManager().getFilesPFN()

  #############################################################################
  def getNumberOfEvents( self, files ):
    """number of events"""
    return self.getManager().getNumberOfEvents( files )

  #############################################################################
  def writeJobOptions( self, files, optionsFile = "jobOptions.opts",
                      savedType = None, catalog = None, savePfn = None, dataset = None ):
    """Gaudi card"""
    return self.getManager().writeJobOptions( files, optionsFile, savedType, catalog, savePfn, dataset )

  #############################################################################
  def getJobInfo( self, lfn ):
    """ how a file is created"""
    return self.getManager().getJobInfo( lfn )

  #############################################################################
  def setVerbose( self, value ):
    """only important information"""
    return self.getManager().setVerbose( value )

  #############################################################################
  def setAdvancedQueries( self, value ):
    """Advanced queries"""
    return self.getManager().setAdvancedQueries( value )

  #############################################################################
  def getLimitedFiles( self, selectionDict, sortDict, startItem, maxitems ):
    """get files used by Web portal"""
    return self.getManager().getLimitedFiles( selectionDict, sortDict, startItem, maxitems )

  #############################################################################
  def getAncestors( self, files, depth ):
    """ ancestor of files"""
    return self.getManager().getAncestors( files, depth )

  #############################################################################
  def getLogfile( self, filename ):
    """ log file of a given file"""
    return self.getManager().getLogfile( filename )

  #############################################################################
  def writePythonOrJobOptions( self, startItem, maxitems, path, optstype ):
    """python job option"""
    return self.getManager().writePythonOrJobOptions( startItem, maxitems, path, optstype )

  #############################################################################
  def getLimitedInformations( self, startItem, maxitems, path ):
    """statistics"""
    return self.getManager().getLimitedInformations( startItem, maxitems, path )

  #############################################################################
  def getProcessingPassSteps( self, in_dict ):
    """step"""
    return self.getManager().getProcessingPassSteps( in_dict )

  #############################################################################
  def getMoreProductionInformations( self, prodid ):
    """production details"""
    return self.getManager().getMoreProductionInformations( prodid )

  #############################################################################
  def getAvailableProductions( self ):
    """available productions"""
    return self.getManager().getAvailableProductions()

  #############################################################################
  def getFileHistory( self, lfn ):
    """"file history"""
    return self.getManager().getFileHistory( lfn )

  #############################################################################
  def getCurrentParameter( self ):
    """ curent bookkeeping path"""
    return self.getManager().getCurrentParameter()

  #############################################################################
  def getQueriesTypes( self ):
    """type of the current query"""
    return self.getManager().getQueriesTypes()

  #############################################################################
  def getProductionProcessingPassSteps( self, in_dict ):
    """the steps which produced a given production"""
    return self.getManager().getProductionProcessingPassSteps( in_dict )

  #############################################################################
  def getAvailableDataQuality( self ):
    """available data quality"""
    return self.getManager().getAvailableDataQuality()

  #############################################################################
  def setDataQualities( self, values ):
    """set data qualities"""
    self.getManager().setDataQualities( values )

  #############################################################################
  def getStepsMetadata( self, bkDict ):
    """returns detailed step metadata """
    return self.getManager().getStepsMetadata( bkDict )
  
  #############################################################################
  def setFileTypes( self, fileTypeList ):
    """it sets the file types"""
    return self.getManager().setFileTypes( fileTypeList )
  

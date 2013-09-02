""" Client module to deal with transformations, but mostly dedicated to DataManipulation (e.g.: replications)
"""

__RCSID__ = "$Id$"

from DIRAC                                                      import gLogger, S_OK
from DIRAC.TransformationSystem.Client.Transformation           import Transformation as DIRACTransformation

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient

COMPONENT_NAME = 'Transformation'

class Transformation( DIRACTransformation ):
  """ class - mostly for DM prods
  """

  #############################################################################

  def __init__( self, transID = 0, transClientIn = None ):
    """ Just params setting.
        transClient is passed here as LHCbDIRAC TransformationsClient, it will be self.transClient
    """

    if not transClientIn:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClientIn

    DIRACTransformation.__init__( self, transID = transID, transClient = self.transClient )

    if not  self.paramValues.has_key( 'BkQuery' ):
      self.paramValues['BkQuery'] = {}
    if not self.paramValues.has_key( 'BkQueryID' ):
      self.paramValues['BkQueryID'] = 0

  #############################################################################

  def testBkQuery( self, bkQuery, printOutput = False, bkClient = None ):
    """ just pretty print of the result of a BK Query
    """

    if bkClient is None:
      bkClient = BookkeepingClient()

    res = bkClient.getFiles( bkQuery )
    if not res['OK']:
      return self._errorReport( res, 'Failed to perform BK query' )
    gLogger.info( 'The supplied query returned %d files' % len( res['Value'] ) )
    if printOutput:
      self._prettyPrint( res )
    return S_OK( res['Value'] )

  #############################################################################

  def setBkQuery( self, queryDict, test = False ):
    """ set a BKK Query
    """
    if test:
      res = self.testBkQuery( queryDict )
      if not res['OK']:
        return res
    transID = self.paramValues['TransformationID']
    if self.exists and transID:
      res = self.transClient.createTransformationQuery( transID, queryDict )
      if not res['OK']:
        return res
      self.item_called = 'BkQueryID'
      self.paramValues[self.item_called] = res['Value']
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = queryDict
    return S_OK()

  #############################################################################

  def getBkQuery( self, printOutput = False ):
    """ get a BKK Query
    """
    if self.paramValues['BkQuery']:
      return S_OK( self.paramValues['BkQuery'] )
    res = self.__executeOperation( 'getBookkeepingQueryForTransformation', printOutput = printOutput )
    if not res['OK']:
      return res
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = res['Value']
    return S_OK( res['Value'] )

  #############################################################################

  def deleteTransformationBkQuery( self ):
    """ delete a BKK Query
    """
    if not self.paramValues['BkQueryID']:
      gLogger.info( "The BK Query is not defined" )
      return S_OK()
    transID = self.paramValues['TransformationID']
    if self.exists and transID:
      res = self.transClient.deleteTransformationBookkeepingQuery( transID )
      if not res['OK']:
        return res
    self.item_called = 'BkQueryID'
    self.paramValues[self.item_called] = 0
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = {}
    return S_OK()

  #############################################################################

  def addTransformation( self, addFiles = True, printOutput = False ):
    """ Add a transformation, using TransformationClient()
    """
    res = self._checkCreation()
    if not res['OK']:
      return self._errorReport( res, 'Failed transformation sanity check' )
    if printOutput:
      gLogger.info( "Will attempt to create transformation with the following parameters" )
      self._prettyPrint( self.paramValues )

    bkQuery = self.paramValues['BkQuery']
    if bkQuery:
      res = self.setBkQuery( bkQuery )
      if not res['OK']:
        return self._errorReport( res, 'Failed BK query sanity check' )

    res = self.transClient.addTransformation( self.paramValues['TransformationName'],
                                              self.paramValues['Description'],
                                              self.paramValues['LongDescription'],
                                              self.paramValues['Type'],
                                              self.paramValues['Plugin'],
                                              self.paramValues['AgentType'],
                                              self.paramValues['FileMask'],
                                              transformationGroup = self.paramValues['TransformationGroup'],
                                              groupSize = self.paramValues['GroupSize'],
                                              inheritedFrom = self.paramValues['InheritedFrom'],
                                              body = self.paramValues['Body'],
                                              maxTasks = self.paramValues['MaxNumberOfTasks'],
                                              eventsPerTask = self.paramValues['EventsPerTask'],
                                              addFiles = addFiles,
                                              bkQuery = self.paramValues['BkQuery'] )
    if not res['OK']:
      if printOutput:
        self._prettyPrint( res )
      return res
    transID = res['Value']
    if self.paramValues['BkQuery']:
      res = self.transClient.getTransformationParameters( transID, ['BkQueryID'] )
      if not res['OK']:
        if printOutput:
          self._prettyPrint( res )
        return res
      self.setBkQueryID( res['Value'] )
    self.exists = True
    self.setTransformationID( transID )
    gLogger.info( "Created transformation %d" % transID )
    for paramName, paramValue in self.paramValues.items():
      if not self.paramTypes.has_key( paramName ):
        if not paramName in ['BkQueryID', 'BkQuery']:
          # print paramName, type( paramValue ), paramValue
          # Use str(paramValue) as a temporary fix???
          res = self.transClient.setTransformationParameter( transID, paramName, str( paramValue ) )
          if not res['OK']:
            gLogger.error( "Failed to add parameter", "%s %s" % ( paramName, res['Message'] ) )
            gLogger.info( "To add this parameter later please execute the following." )
            gLogger.info( "oTransformation = Transformation(%d)" % transID )
            gLogger.info( "oTransformation.set%s(...)" % paramName )
    return S_OK()

  def setSEParam( self, key, seList ):
    return self.__setSE( key, seList )

  def setAdditionalParam( self, key, val ):
    self.item_called = key
    return self.__setParam( val )

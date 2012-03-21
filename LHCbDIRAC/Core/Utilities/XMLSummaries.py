""" Utilities to check the XML summary files  
"""

__RCSID__ = "$Id$"

import os
from DIRAC import S_OK, S_ERROR
from LHCbDIRAC.Core.Utilities.XMLTreeParser import XMLTreeParser


class XMLSummaryError( Exception ):

  def __init__( self, message = "" ):

    self.message = message
    Exception.__init__( self, message )

  def __str__( self ):
    return "XMLSummaryError:" + repr( self.message )


################################################################################

class XMLSummary:

  def __init__( self, xmlFileName, log = None ):
    """ initialize a XML summary object, given a fileName, getting some relevant info
    """

    if not log:
      from DIRAC import gLogger
      self.log = gLogger.getSubLogger( 'XMLSummary' )
    else:
      self.log = log

    self.xmlFileName = xmlFileName

    if not os.path.exists( self.xmlFileName ):
      raise XMLSummaryError, 'XML Summary Not Available'

    if os.stat( self.xmlFileName )[6] == 0:
      raise XMLSummaryError, 'Requested XML summary file "%s" is empty' % self.xmlFileName

    summary = XMLTreeParser()

    try:
      self.xmlTree = summary.parse( self.xmlFileName )
    except Exception, e:
      raise XMLSummaryError, 'Error parsing xml summary: %s' % str( e )

    self.success = self.__getSuccess()
    self.step = self.__getStep()
    self.memory = self.__getMemory()
    self.inputFileStats = self.__getInputFileStats()
    self.inputEventsTotal, self.inputsEvents = self.__getInputEvents()
    self.outputFileStats = self.__getOutputFileStats()
    self.outputEventsTotal, self.outputsEvents = self.__getOutputEvents()

################################################################################

  def analyse( self, inputsOnPartOK = False ):
    """ analyse the XML summary
    """
    if inputsOnPartOK:
      self.log.warn( 'part status for input files is considered OK' )
    if self.success == 'True' and self.step == 'finalize' \
    and self._inputsOK( inputsOnPartOK ) and self._outputsOK():
      return S_OK( 'XML Summary OK' )
    else:
      return S_ERROR( 'XML Summary reports errors' )

################################################################################

  def _inputsOK( self, inputsOnPartOK = False ):
    """ check self.inputFileStats
    """

    if inputsOnPartOK:
      if sum( self.inputFileStats.values() ) == self.inputFileStats['part'] or \
      sum( self.inputFileStats.values() ) == self.inputFileStats['full']:
        return True
      else:
        return False

    else:
      if sum( self.inputFileStats.values() ) == self.inputFileStats['full']:
        return True
      else:
        return False

################################################################################

  def _outputsOK( self ):
    """ check self.outputFileStats
    """

    if sum( self.outputFileStats.values() ) == self.outputFileStats['full']:
      return True
    else:
      return False

################################################################################

  def __getSuccess( self ):
    """get the success
    """

    sum = self.xmlTree[ 0 ]

    successXML = sum.childrens( 'success' )
    if len( successXML ) != 1:
      raise XMLSummaryError, 'XMLSummary bad formatted: Nr of success items != 1'

    return successXML[ 0 ].value

################################################################################

  def __getStep( self ):
    """Get the step
    """

    sum = self.xmlTree[ 0 ]

    stepXML = sum.childrens( 'step' )
    if len( stepXML ) != 1:
      raise XMLSummaryError, 'XMLSummary bad formatted: Nr of step items != 1'

    return stepXML[ 0 ].value

################################################################################

  def __getMemory( self ):
    """get the memory used
    """

    sum = self.xmlTree[ 0 ]

    statXML = sum.childrens( 'usage' )
    if len( statXML ) != 1:
      raise XMLSummaryError, 'XMLSummary bad formatted: Nr of step items != 1'

    statXML = statXML[0].childrens( 'stat' )

    return statXML[ 0 ].value

################################################################################

  def __getInputStatus( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < input >
        ...
    '''

    files = []

    sum = self.xmlTree[ 0 ]

    for input in sum.childrens( 'input' ):
      for file in input.childrens( 'file' ):
        try:
          files.append( ( file.attributes[ 'name' ], file.attributes[ 'status' ] ) )
        except Exception:
          raise XMLSummaryError, 'Bad formatted file keys'

    return files

################################################################################

  def __getInputFileStats( self ):
    """Checks that every input file has reached the full status.
       Four possible statuses of the files:
       - full : the file has been fully read
       - part : the file has been partially read
       - mult : the file has been read multiple times
       - fail : failure while reading the file
    """

    res = self.__getInputStatus()

    fileCounter = {
                   'full'  : 0,
                   'part'  : 0,
                   'mult'  : 0,
                   'fail'  : 0,
                   'other' : 0
                   }

    for fileIn, status in res:

      if status == 'fail':
        self.log.error( 'Input File %s is on status %s.' % ( fileIn, status ) )
        fileCounter[ 'fail' ] += 1

      elif status == 'mult':
        self.log.error( 'Input File %s is on status %s.' % ( fileIn, status ) )
        fileCounter[ 'mult' ] += 1

      elif status == 'part':
        self.log.error( 'Input File %s is on status %s.' % ( fileIn, status ) )
        fileCounter[ 'part' ] += 1

      elif status == 'full':
        #If it is Ok, we do not print anything
        #self.log.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'full' ] += 1

      # This should never happen, but just in case
      else:
        self.log.error( 'Input File %s is on unknown status: %s' % ( fileIn, status ) )
        fileCounter[ 'other'] += 1

    files = [ '%d input file(s) on %s status' % ( v, k ) for k, v in fileCounter.items() if v > 0 ]
    filesMsg = ', '.join( files )
    self.log.info( filesMsg )

    return fileCounter

################################################################################

  def __getInputEvents( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < input >
        ...
    '''

    inputEventsTotal = 0
    inputsEvents = {}

    sum = self.xmlTree[ 0 ]

    for output in sum.childrens( 'input' ):
      for fileIn in output.childrens( 'file' ):
        inputEventsTotal += int( fileIn.value )
        inputsEvents[fileIn.attributes['name'].replace( 'LFN:', '' ).replace( 'PFN:', '' ).split( '/' ).pop()] = fileIn.value

    return inputEventsTotal, inputsEvents

################################################################################

  def __getOutputStatus( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < output >
        ...
    '''

    files = []

    sum = self.xmlTree[ 0 ]

    for output in sum.childrens( 'output' ):
      for fileIn in output.childrens( 'file' ):
        try:
          files.append( ( fileIn.attributes[ 'name' ], fileIn.attributes[ 'status' ] ) )
        except Exception:
          raise XMLSummaryError, 'Bad formatted file keys. %s'

    return files

################################################################################

  def __getOutputEvents( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < output >
        ...
    '''

    outputEventsTotal = 0
    outputsEvents = {}

    sum = self.xmlTree[ 0 ]

    for output in sum.childrens( 'output' ):
      for fileIn in output.childrens( 'file' ):
        outputEventsTotal += int( fileIn.value )
        outputsEvents[fileIn.attributes['name'].replace( 'LFN:', '' ).replace( 'PFN:', '' ).split( '/' ).pop()] = fileIn.value

    return outputEventsTotal, outputsEvents

################################################################################

  def __getOutputFileStats( self ):
    """Checks that every output file has reached the full status.
       Four possible statuses of the files:
       - full : the file has been fully read
       - part : the file has been partially read
       - mult : the file has been read multiple times
       - fail : failure while reading the file
    """

    res = self.__getOutputStatus()

    fileCounter = {
                   'full'  : 0,
                   'part'  : 0,
                   'mult'  : 0,
                   'fail'  : 0,
                   'other' : 0
                   }

    for file, status in res:

      if status == 'fail':
        self.log.error( 'Output File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'fail' ] += 1

      elif status == 'mult':
        self.log.error( 'Output File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'mult' ] += 1

      elif status == 'part':
        self.log.error( 'Output File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'part' ] += 1

      elif status == 'full':
        #If it is Ok, we do not print anything
        #self.log.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'full' ] += 1

      # This should never happen, but just in case
      else:
        self.log.error( 'Output File %s is on unknown status: %s' % ( file, status ) )
        fileCounter[ 'other'] += 1

    files = [ '%d output file(s) on %s status' % ( v, k ) for k, v in fileCounter.items() if v > 0 ]
    filesMsg = ', '.join( files )
    self.log.info( filesMsg )

    return fileCounter

################################################################################

def analyseXMLSummary( xmlFileName = None, xf_o = None, log = None, inputsOnPartOK = False ):
  """ Analyse a XML summary file
  """

  try:

    if not xf_o:
      xf_o = XMLSummary( xmlFileName, log = log )
    return xf_o.analyse( inputsOnPartOK )

  except XMLSummaryError, e:
    return S_ERROR ( str( e ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

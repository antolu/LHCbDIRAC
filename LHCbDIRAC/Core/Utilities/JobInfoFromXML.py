########################################################################
# File :   JobOutputLFN.py
# Author : Vladimir Romanovsky
########################################################################
""" Return the INFO a a given job from the XML file
"""

__RCSID__ = "$Id: JobInfoFromXML.py 76566 2014-06-26 09:48:30Z fstagni $"

from DIRAC import S_OK, S_ERROR
import shutil, os

# They should not be here, but I do not know their effect in terms of load.
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.Interfaces.API.LHCbJob   import LHCbJob

def makeProductionLFN( jobid, prodid, config, fname, ftype ):
  """ Constructs the logical file name according to LHCb conventions.
  Returns the lfn without 'lfn:' prepended
  """

  if fname.count( 'lfn:' ):
    return fname.replace( 'lfn:', '' )

  if fname.count( 'LFN:' ):
    return fname.replace( 'LFN:', '' )

  if config.count( 'DC06' ):
    lfnroot = '/lhcb/MC/DC06'
  else:
    lfnroot = '/lhcb/data/'

  try:
    jobindex = "%04d" % ( int( jobid ) / 10000 )
  except Exception:
    jobindex = '0000'

  return os.path.join( lfnroot, str( ftype ).upper(), prodid, jobindex, fname )


class JobInfoFromXML( object ):
  """ main class"""

  def __init__( self, jobid ):

    self.message = None
    try:
      job = int( jobid )
    except Exception:
      self.message = 'Input parameter is not integer'
      return

    dirac = DiracLHCb()

    result = dirac.getInputSandbox( job )
    if not result['OK']:
      self.message = result['Message']
      return

    try:
      xml = open( 'InputSandbox%s/jobDescription.xml' % job ).read()
    except Exception, x:
      self.message = 'Can not read XML file: %s' % x
      return
    shutil.rmtree( 'InputSandbox%s' % job )

    self.j = LHCbJob( xml )
    self.jobid = None
    self.prodid = None
    self.jobname = None
    self.output = None
    self.inputdata = None
    self.configversion = None

    for p in self.j.workflow.parameters:
      if p.getName() == "JOB_ID":
        self.jobid = p.getValue()
      if p.getName() == "PRODUCTION_ID":
        self.prodid = p.getValue()
      if p.getName() == "JobName":
        self.jobname = p.getValue()
      if p.getName() == "outputDataFileMask":
        self.output = p.getValue()
      if p.getName() == "InputData":
        self.inputdata = p.getValue()
      if p.getName() == "configVersion":
        self.configversion = p.getValue()

    if not self.jobid or not self.prodid or not self.jobname or not self.configversion:
      self.message = 'Wrong job parameters: %s' % str( {'JOB_ID':self.jobid, 'PRODUCTION_ID':self.prodid, 'JobName':self.jobname, 'configVersion':self.configversion} )
      return

  def valid( self ):
    """ Check the validity of message"""
    if self.message:
      return S_ERROR( self.message )
    return S_OK()

  def getInputLFN( self ):
    """ return the list of LFNS for the job """
    if self.message:
      return S_ERROR( self.message )

    if not self.inputdata:
      return S_OK( [] )

    jobid = self.jobid
    prodid = self.prodid
    configversion = self.configversion
    filename = self.inputdata
    filetype = None
    inputlfns = [makeProductionLFN( jobid, prodid, configversion, filename, filetype )]
    return S_OK( inputlfns )

  def getOutputLFN( self ):
    """  get the list of LFNs for the output """
    if self.message:
      return S_ERROR( self.message )

    code = self.j.workflow.createCode()
    listoutput = []
    for line in code.split( "\n" ):
      if line.count( "listoutput" ):
        listoutput += eval( line.split( "#" )[0].split( "=" )[-1] )

    outputlfns = []
    for item in listoutput:
      if ( not self.output ) or item['outputDataType'] in self.output:
        jobid = self.jobid
        prodid = self.prodid
        configversion = self.configversion
        filename = item['outputDataName']
        filetype = item['outputDataType']
        lfn = makeProductionLFN( jobid, prodid, configversion, filename, filetype )
        outputlfns.append( lfn )

    return S_OK( outputlfns )

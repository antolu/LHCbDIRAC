"""
  CVMFSChech module for SAM jobs
"""

import os

# DIRAC
from DIRAC import gLogger, S_ERROR, S_OK

# LHCbDIRAC
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

__RCSID__ = '$Id: $'

class CVMFSCheck( ModuleBase ):
  """
  CVMFSCheck module extending more generic LHCb jobs ModuleBase.
  """
  
  def __init__( self ):
    """
    Constructor
    """
    super( CVMFSCheck, self ).__init__()
    
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

  def execute( self ):
    """
    Main method. If ${VO_LHCB_SW_DIR}/lib/etc/cernvmfs is present, we continue
    the SAM job.
    """
    
    self.log.info( 'Checking presence of CVMFS' )
    
    if not 'VO_LHCB_SW_DIR' in os.environ:
      self.setApplicationStatus( 'CVMFS KO' )
      self.log.error( 'Environment variable VO_LHCB_SW_DIR not found' )
      return S_ERROR( 'Environment variable VO_LHCB_SW_DIR not found' )
    
    swDir = os.environ[ 'VO_LHCB_SW_DIR' ]
    
    cvmfsFilePath = os.path.join( swDir, '/lib/etc/cernvmfs' )
    self.log.info( 'CVMFS file path "%s"' % cvmfsFilePath )
    
    if not os.path.exists( cvmfsFilePath ):
      self.setApplicationStatus( 'CVMFS KO' )
      self.log.error( 'CVMFS file path "%s" does not exist' % cvmfsFilePath )
      return S_ERROR( 'CVMFS file path "%s" does not exist' % cvmfsFilePath )
    
    self.setApplicationStatus( 'CVMFS OK' )
    self.log.info( 'CVMFS is present' )
    return S_OK( 'CVMFS is present' )    

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
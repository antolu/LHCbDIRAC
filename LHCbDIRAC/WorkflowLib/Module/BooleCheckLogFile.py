########################################################################
# $Id: BooleCheckLogFile.py,v 1.7 2008/02/27 14:04:05 joel Exp $
########################################################################
""" Script Base Class """


__RCSID__ = "$Id: BooleCheckLogFile.py,v 1.7 2008/02/27 14:04:05 joel Exp $"


import os,string

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from WorkflowLib.Module.LHCbCheckLogFile import LHCbCheckLogFile
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from WorkflowLib.Module.CheckLogFile import CheckLogFile
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig

class BooleCheckLogFile(LHCbCheckLogFile):

  def __init__(self):
    self.argv0     = 'BooleCheckLogFile'
    self.log = gLogger.getSubLogger("BooleCheckLogFile")
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    self.iClient   = None
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.info      = 1

  def checkApplicationLog(self,error):
    if not os.path.exists(self.appLog):
      return S_ERROR(error)

    self.log.info('analyze %s ' % (self.appLog))

    mailto = self.appName.upper()+'_EMAIL'

    last_line = self.grep(self.logfile,'dum','-l')
    if string.find(last_line,'User defined signal') != -1:
      mailto = 'DIRAC_EMAIL'
      result = S_ERROR(mailto + ' USER signal')
    else:
      result = S_ERROR(error)

    if result['Message'].find('EMAIL') != -1:
      self.sendErrorMail(result['Message'])

    return result
#
#------------------------------------------------------------------------------------------
#
if __name__ == '__main__':

   Chk = BooleCheckLogFile()

   rc = Chk.execute()

   print 'main-Boole - rc= ',rc



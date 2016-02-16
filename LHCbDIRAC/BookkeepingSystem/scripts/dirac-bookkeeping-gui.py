#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-gui.py
# Author :  Zoltan Mathe
########################################################################
"""
  Command to invoke the LHCb Bookkeeping Database graphical user interface
"""
__RCSID__ = "$Id: dirac-bookkeeping-gui.py 85405 2015-08-28 13:42:52Z joel $"

from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ]))

Script.parseCommandLine(ignoreErrors=True)

from DIRAC.Core.Security.ProxyInfo                             import getProxyInfo
from DIRAC.Core.Security.CS                                    import getDNForUsername
from DIRAC                                                     import gLogger
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.MainWidget         import MainWidget
from PyQt4.QtGui import QApplication


import sys

class BookkeepingApplication(QApplication):
  """
  The Bookkeeping standalone gui application class
  """
  def __init__(self, args):
    """initialize the class members"""
    QApplication.__init__(self, args)
    fileName = ''
    savePath = ''
    if len(args) > 1:
      for i in range(1, len(args)):
        arg = args[i]
        opts = arg.split('=')
        if len(opts) == 1:
          fileName = opts[0]
        if opts[0] == 'txt':
          fileName = opts[1]
        elif opts[0] == 'ds':
          savePath = opts[1]
        else:
          print 'ERORR: Argument error!!'
    self.mainWidget = MainWidget(fileName=fileName, savepath=savePath)
    #self.mainWidget.setWindowFlags( Qt.WindowTitleHint | Qt.WindowMinimizeButtonHint | Qt.WindowSystemMenuHint)
    self.mainWidget.show()
    self.mainWidget.start()
if __name__ == "__main__":
  res = getProxyInfo(False, False)
  if not res['OK']:
    gLogger.error("Failed to get client proxy information.", res['Message'])
    sys.exit(1)
  proxyInfo = res['Value']
  if not proxyInfo.has_key('group'):
    errStr = "Proxy information does not contain the group."
    gLogger.error(errStr)
    sys.exit(1)
  res = getDNForUsername(proxyInfo['username'])
  if not res['OK']:
    errStr = "ReplicaManager.__getClientCertGroup: Error getting known proxies for user."
    gLogger.error(errStr, res['Message'])
    sys.exit(1)

  application = BookkeepingApplication(sys.argv)
  sys.exit(application.exec_())


#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-gui.py,v 1.10 2009/08/14 15:01:08 zmathe Exp $
# File :   dirac-bookkeeping-gui.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-gui.py,v 1.10 2009/08/14 15:01:08 zmathe Exp $"
__VERSION__ = "$ $"
from DIRAC.Core.Security.Misc                              import getProxyInfo
from DIRAC.Core.Security.CS import getDNForUsername
from DIRAC                                                 import gLogger, S_OK, S_ERROR
from DIRACEnvironment                                      import DIRAC
from DIRAC.BookkeepingSystem.Gui.Widget.MainWidget         import MainWidget
from PyQt4.QtGui import *
from PyQt4.QtCore import *

import sys

class bkk(QApplication):
    
    def __init__(self, args):
        
        QApplication.__init__(self,args)
        fileName = ''
        savePath = ''
        if len(args) > 1:
          for i in range(1,len(args)):
            arg = args[i]
            opts = arg.split('=')
            if len(opts) == 1:
              fileName=opts[0]
            if opts[0]=='txt':
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
    res = getProxyInfo(False,False)
    if not res['OK']:
      gLogger.error("Failed to get client proxy information.",res['Message'])  
      sys.exit(1)
    proxyInfo = res['Value']
    if not proxyInfo.has_key('group'):
      errStr = "Proxy information does not contain the group."
      gLogger.error(errStr)
      sys.exit(1)
    res = getDNForUsername(proxyInfo['username'])
    if not res['OK']:
      errStr = "ReplicaManager.__getClientCertGroup: Error getting known proxies for user."
      gLogger.error(errStr,res['Message'])
      sys.exit(1)
      
    app = bkk(sys.argv)
    sys.exit(app.exec_())
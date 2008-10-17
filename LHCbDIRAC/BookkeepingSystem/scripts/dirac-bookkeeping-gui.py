#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-gui.py,v 1.3 2008/10/17 13:03:02 rgracian Exp $
# File :   dirac-bookkeeping-gui.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-gui.py,v 1.3 2008/10/17 13:03:02 rgracian Exp $"
__VERSION__ = "$ $"
from DIRACEnvironment import DIRAC
from DIRAC.BookkeepingSystem.Gui.Widget.MainWidget import MainWidget
from PyQt4.QtGui import *
from PyQt4.QtCore import *

import sys

class bkk(QApplication):
    
    def __init__(self, args):
        
        QApplication.__init__(self,args)
        
        self.mainWidget = MainWidget()
        self.mainWidget.setWindowFlags( Qt.WindowTitleHint | Qt.WindowMinimizeButtonHint | Qt.WindowSystemMenuHint)
        self.mainWidget.show()
        self.mainWidget.start()

if __name__ == "__main__":
    app = bkk(sys.argv)
    sys.exit(app.exec_())
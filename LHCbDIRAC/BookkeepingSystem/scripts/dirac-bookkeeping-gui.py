
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
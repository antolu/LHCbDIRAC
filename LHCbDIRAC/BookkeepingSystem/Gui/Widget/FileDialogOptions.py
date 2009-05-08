########################################################################
# $Id: FileDialogOptions.py,v 1.1 2009/05/08 15:23:24 zmathe Exp $
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *

from DIRAC.BookkeepingSystem.Gui.Widget.FileDialogOptions_ui    import Ui_FileDialogOptions

import types

__RCSID__ = "$Id: FileDialogOptions.py,v 1.1 2009/05/08 15:23:24 zmathe Exp $"

#############################################################################  
class FileDialogOptions(QDialog, Ui_FileDialogOptions):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = parent.getControler()
    self.connect(self.okButton, SIGNAL("clicked()"), self.__controler.ok)
    self.connect(self.cancelButton, SIGNAL("clicked()"), self.__controler.cancel)
    

  #############################################################################  
  def getControler(self):
    return self.__controler
  
  
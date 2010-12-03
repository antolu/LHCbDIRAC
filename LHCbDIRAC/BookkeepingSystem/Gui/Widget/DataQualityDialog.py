########################################################################
# $Id: $
########################################################################
from PyQt4.QtGui                                                              import *
from PyQt4.QtCore                                                             import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.DataQualityDialog_ui              import Ui_DataQualityDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerDataQualityDialog     import ControlerDataQualityDialog
import LHCbDIRAC,os

__RCSID__ = "$ $"

#############################################################################  
class DataQualityDialog(QDialog, Ui_DataQualityDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerDataQualityDialog(self, parent.getControler())
    
    self.connect(self.OkButton, SIGNAL("clicked()"), self.__controler.close)
    
    picturesPath = os.path.dirname(os.path.realpath(LHCbDIRAC.__path__[0]))+'/LHCbDIRAC/BookkeepingSystem/Gui/Widget'
    OkICon = QIcon(picturesPath+"/images/ok.png")
    self.OkButton.setIcon(OkICon)
    self.__checkboses = []
        
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################
  def getCheckBoses(self):
    return self.__checkboses
  
  #############################################################################
  def addDataQulity(self, values):
    
    self.__checkboses = []
    j = 0
    for i in values:
      self.__checkboses.append(QCheckBox(self.groupBox))
      self.__checkboses[j].setObjectName("checkBox")
      self.gridlayout1.addWidget(self.__checkboses[j],j + 1,0,1,1)
      self.__checkboses[j].setText(QApplication.translate("DataQualityDialog", i, None, QApplication.UnicodeUTF8))
      self.__checkboses[j].setChecked(values[i])
      j += 1
      

                
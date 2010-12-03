########################################################################
# $Id: AdvancedSave.py 28101 2010-08-23 15:35:32Z zmathe $
########################################################################
from PyQt4.QtGui                                                              import *
from PyQt4.QtCore                                                             import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.AdvancedSave_ui                   import Ui_Dialog
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAdvancedSave          import ControlerAdvancedSave

import DIRAC,os

__RCSID__ = "$Id: AdvancedSave.py 28101 2010-08-23 15:35:32Z zmathe $"

#############################################################################  
class AdvancedSave(QDialog, Ui_Dialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)

  
    self.__controler = ControlerAdvancedSave(self, parent.getControler())
    self.setLFNbutton()
    
    self.connect(self.lfnButton, SIGNAL("clicked()"), self.__controler.lfnButtonChanged)
    self.connect(self.pfnButton, SIGNAL("clicked()"), self.__controler.pfnButtonChanged)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.saveButton)
  
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def fillWindows(self, sites):
    self.comboBox.clear()
    j = 0
    for i in sites:
      self.comboBox.addItem (i, QVariant(str(sites[i])))
      if i == 'Select a site':
        self.comboBox.setCurrentIndex(j)
      j += 1
                   
  #############################################################################  
  def setLFNbutton(self):
    if self.lfnButton.isChecked():
      self.lfnButton.setChecked(False)
    else:
      self.lfnButton.setChecked(True)
  
  #############################################################################  
  def setPFNbutton(self):
    if self.pfnButton.isChecked():
      self.pfnButton.setChecked(False)
    else:
      self.pfnButton.setChecked(True)
  
  #############################################################################  
  def getLineEdit(self):
    return self.lineEdit
  
  #############################################################################  
  def isPFNbuttonChecked(self):
    return self.pfnButton.isChecked()
  
  #############################################################################  
  def isLFNbuttonChecked(self):
    return self.lfnButton.isChecked()
  
  #############################################################################  
  def getSite(self):
    return self.comboBox.currentText()

    
  #############################################################################  
# pylint: skip-file

"""
AdvancedSave widget. It used to create a Gaudi card.
"""
########################################################################
# $Id: AdvancedSave.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################
from PyQt4.QtGui                                                              import QDialog
from PyQt4.QtCore                                                             import SIGNAL, QVariant, Qt

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_AdvancedSave                   import Ui_AdvancedSave
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAdvancedSave          import ControlerAdvancedSave


__RCSID__ = "$Id$"

#############################################################################
class AdvancedSave(QDialog, Ui_AdvancedSave):
  """
  AdvancedSave class
  """

  #############################################################################
  def __init__(self, parent=None):
    """initialize the widget"""
    QDialog.__init__(self, parent)
    Ui_AdvancedSave.__init__(self)
    self.setupUi(self)


    self.__controler = ControlerAdvancedSave(self, parent.getControler())
    self.setLFNbutton()

    self.connect(self.lfnButton, SIGNAL("clicked()"), self.__controler.lfnButtonChanged)
    self.connect(self.pfnButton, SIGNAL("clicked()"), self.__controler.pfnButtonChanged)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.saveButton)

  #############################################################################
  def getControler(self):
    """returns the controller of this widget"""
    return self.__controler

  #############################################################################
  def fillWindows(self, sites):
    """fills the combo box"""
    self.comboBox.clear()
    j = 0
    for i in sites:
      self.comboBox.addItem (i, QVariant(str(sites[i])))
      if i == 'Select a site':
        self.comboBox.setCurrentIndex(j)
      j += 1

  #############################################################################
  def setLFNbutton(self):
    """handles the action when a check box button pressed"""
    if self.lfnButton.isChecked():
      self.lfnButton.setChecked(False)
    else:
      self.lfnButton.setChecked(True)

  #############################################################################
  def setPFNbutton(self):
    """handles the action when a check box button pressed"""
    if self.pfnButton.isChecked():
      self.pfnButton.setChecked(False)
    else:
      self.pfnButton.setChecked(True)

  #############################################################################
  def getLineEdit(self):
    """returns the content of the text box"""
    return self.lineEdit

  #############################################################################
  def isPFNbuttonChecked(self):
    """checks the status of the check box"""
    return self.pfnButton.isChecked()

  #############################################################################
  def isLFNbuttonChecked(self):
    """checks the status of the check box"""
    return self.lfnButton.isChecked()

  #############################################################################
  def getSite(self):
    """returns the selected site"""
    return self.comboBox.currentText()

  #############################################################################
  def waitCursor(self):
    """wait cursor"""
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    """normal cursor"""
    self.setCursor(Qt.ArrowCursor)

  #############################################################################

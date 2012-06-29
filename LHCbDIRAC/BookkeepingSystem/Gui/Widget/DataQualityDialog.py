"""
Datat quality widget
"""
########################################################################
# $Id: $
########################################################################
from PyQt4.QtGui                                                              import QCheckBox, QDialog, QApplication
from PyQt4.QtCore                                                             import SIGNAL

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_DataQualityDialog              import Ui_DataQualityDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerDataQualityDialog     import ControlerDataQualityDialog

__RCSID__ = "$ $"

#############################################################################
class DataQualityDialog(QDialog, Ui_DataQualityDialog):
  """
  DataQualityDialog class
  """
  #############################################################################
  def __init__(self, parent=None):
    """initialize the widget"""
    QDialog.__init__(self, parent)
    Ui_DataQualityDialog.__init__(self)
    self.setupUi(self)
    self.__controler = ControlerDataQualityDialog(self, parent.getControler())

    self.connect(self.OkButton, SIGNAL("clicked()"), self.__controler.close)

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
      self.dataQualityLayout.addWidget(self.__checkboses[j], j + 1, 0, 1, 1)
      self.__checkboses[j].setText(QApplication.translate("DataQualityDialog", i, None, QApplication.UnicodeUTF8))
      self.__checkboses[j].setChecked(values[i])
      j += 1



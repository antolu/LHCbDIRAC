###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
# pylint: skip-file

"""
Data quality widget
"""

from PyQt4.QtGui                                                              import QCheckBox, QDialog, QApplication
from PyQt4.QtCore                                                             import SIGNAL

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_DataQualityDialog              import Ui_DataQualityDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerDataQualityDialog     import ControlerDataQualityDialog

__RCSID__ = "$Id$"

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
    """returns the controller"""
    return self.__controler

  #############################################################################
  def getCheckBoses(self):
    """returns the check boxes"""
    return self.__checkboses

  #############################################################################
  def addDataQulity(self, values):
    """adds the data quality"""
    self.__checkboses = []
    j = 0

    for i in values:
      self.__checkboses.append(QCheckBox(self.groupBox))
      self.__checkboses[j].setObjectName("checkBox")
      self.dataQualityLayout.addWidget(self.__checkboses[j], j + 1, 0, 1, 1)
      self.__checkboses[j].setText(QApplication.translate("DataQualityDialog", i, None, QApplication.UnicodeUTF8))
      self.__checkboses[j].setChecked(values[i])
      j += 1



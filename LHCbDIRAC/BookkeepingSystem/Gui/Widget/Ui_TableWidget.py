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

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TableWidget.ui'
#
# Created: Fri Sep  7 13:27:20 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_TableWidget(object):
  def setupUi(self, TableWidget):
    TableWidget.setObjectName("TableWidget")
    TableWidget.resize(764, 694)
    self.tableWidget = QtGui.QTableWidget(TableWidget)
    self.tableWidget.setGeometry(QtCore.QRect(10, 10, 741, 671))
    self.tableWidget.setObjectName("tableWidget")
    self.tableWidget.setColumnCount(0)
    self.tableWidget.setRowCount(0)

    self.retranslateUi(TableWidget)
    QtCore.QMetaObject.connectSlotsByName(TableWidget)

  def retranslateUi(self, TableWidget):
    TableWidget.setWindowTitle(QtGui.QApplication.translate("TableWidget", "TableWidget", None, QtGui.QApplication.UnicodeUTF8))


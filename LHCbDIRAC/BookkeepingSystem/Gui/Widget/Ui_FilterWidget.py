# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'FilterWidget.ui'
#
# Created: Fri Sep  7 13:24:40 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_FilterWidget(object):
  def setupUi(self, FilterWidget):
    FilterWidget.setObjectName("FilterWidget")
    FilterWidget.resize(247, 183)
    self.gridLayout = QtGui.QGridLayout(FilterWidget)
    self.gridLayout.setObjectName("gridLayout")
    self.lineEdit = QtGui.QLineEdit(FilterWidget)
    self.lineEdit.setObjectName("lineEdit")
    self.gridLayout.addWidget(self.lineEdit, 0, 0, 1, 3)
    self.listView = QtGui.QListView(FilterWidget)
    self.listView.setObjectName("listView")
    self.gridLayout.addWidget(self.listView, 1, 0, 1, 3)
    self.allButton = QtGui.QPushButton(FilterWidget)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/files5.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.allButton.setIcon(icon)
    self.allButton.setObjectName("allButton")
    self.gridLayout.addWidget(self.allButton, 2, 0, 1, 1)
    self.okButton = QtGui.QPushButton(FilterWidget)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(":/icons/images/ok.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.okButton.setIcon(icon1)
    self.okButton.setObjectName("okButton")
    self.gridLayout.addWidget(self.okButton, 2, 1, 1, 1)

    self.retranslateUi(FilterWidget)
    QtCore.QMetaObject.connectSlotsByName(FilterWidget)

  def retranslateUi(self, FilterWidget):
    FilterWidget.setWindowTitle(QtGui.QApplication.translate("FilterWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
    self.allButton.setText(QtGui.QApplication.translate("FilterWidget", "All", None, QtGui.QApplication.UnicodeUTF8))
    self.okButton.setText(QtGui.QApplication.translate("FilterWidget", "ApplyFilter", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc

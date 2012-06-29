# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/FilterWidget.ui'
#
# Created: Fri Jun 29 16:01:14 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_FilterWidget(object):
  def setupUi(self, FilterWidget):
    FilterWidget.setObjectName(_fromUtf8("FilterWidget"))
    FilterWidget.resize(247, 183)
    self.gridLayout = QtGui.QGridLayout(FilterWidget)
    self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
    self.lineEdit = QtGui.QLineEdit(FilterWidget)
    self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
    self.gridLayout.addWidget(self.lineEdit, 0, 0, 1, 3)
    self.listView = QtGui.QListView(FilterWidget)
    self.listView.setObjectName(_fromUtf8("listView"))
    self.gridLayout.addWidget(self.listView, 1, 0, 1, 3)
    self.allButton = QtGui.QPushButton(FilterWidget)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/files5.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.allButton.setIcon(icon)
    self.allButton.setObjectName(_fromUtf8("allButton"))
    self.gridLayout.addWidget(self.allButton, 2, 0, 1, 1)
    self.okButton = QtGui.QPushButton(FilterWidget)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/ok.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.okButton.setIcon(icon1)
    self.okButton.setObjectName(_fromUtf8("okButton"))
    self.gridLayout.addWidget(self.okButton, 2, 1, 1, 1)

    self.retranslateUi(FilterWidget)
    QtCore.QMetaObject.connectSlotsByName(FilterWidget)

  def retranslateUi(self, FilterWidget):
    FilterWidget.setWindowTitle(QtGui.QApplication.translate("FilterWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
    self.allButton.setText(QtGui.QApplication.translate("FilterWidget", "All", None, QtGui.QApplication.UnicodeUTF8))
    self.okButton.setText(QtGui.QApplication.translate("FilterWidget", "ApplyFilter", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc

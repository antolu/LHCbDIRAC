# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/HistoryDialog.ui'
#
# Created: Fri Jun 29 16:01:43 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_HistoryDialog(object):
  def setupUi(self, HistoryDialog):
    HistoryDialog.setObjectName(_fromUtf8("HistoryDialog"))
    HistoryDialog.resize(909, 498)
    self.gridLayout = QtGui.QGridLayout(HistoryDialog)
    self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
    self.backButton = QtGui.QPushButton(HistoryDialog)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/back.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.backButton.setIcon(icon)
    self.backButton.setObjectName(_fromUtf8("backButton"))
    self.gridLayout.addWidget(self.backButton, 1, 0, 1, 1)
    self.nextButton = QtGui.QPushButton(HistoryDialog)
    self.nextButton.setEnabled(True)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/next.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.nextButton.setIcon(icon1)
    self.nextButton.setObjectName(_fromUtf8("nextButton"))
    self.gridLayout.addWidget(self.nextButton, 1, 1, 1, 1)
    self.closeButton = QtGui.QPushButton(HistoryDialog)
    icon2 = QtGui.QIcon()
    icon2.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/close.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.closeButton.setIcon(icon2)
    self.closeButton.setObjectName(_fromUtf8("closeButton"))
    self.gridLayout.addWidget(self.closeButton, 1, 2, 1, 1)
    self.groupBox = QtGui.QGroupBox(HistoryDialog)
    self.groupBox.setTitle(_fromUtf8(""))
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.gridLayout_3 = QtGui.QGridLayout(self.groupBox)
    self.gridLayout_3.setObjectName(_fromUtf8("gridLayout_3"))
    self.jobTableView = QtGui.QTableView(self.groupBox)
    self.jobTableView.setObjectName(_fromUtf8("jobTableView"))
    self.gridLayout_3.addWidget(self.jobTableView, 1, 1, 1, 1)
    self.groupBox_2 = QtGui.QGroupBox(self.groupBox)
    self.groupBox_2.setTitle(_fromUtf8(""))
    self.groupBox_2.setObjectName(_fromUtf8("groupBox_2"))
    self.gridLayout_2 = QtGui.QGridLayout(self.groupBox_2)
    self.gridLayout_2.setObjectName(_fromUtf8("gridLayout_2"))
    self.filesTableView = QtGui.QTableView(self.groupBox_2)
    self.filesTableView.setObjectName(_fromUtf8("filesTableView"))
    self.gridLayout_2.addWidget(self.filesTableView, 0, 0, 1, 1)
    self.gridLayout_3.addWidget(self.groupBox_2, 1, 0, 1, 1)
    self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 3)

    self.retranslateUi(HistoryDialog)
    QtCore.QMetaObject.connectSlotsByName(HistoryDialog)

  def retranslateUi(self, HistoryDialog):
    HistoryDialog.setWindowTitle(QtGui.QApplication.translate("HistoryDialog", "Feicim File History dialog window", None, QtGui.QApplication.UnicodeUTF8))
    self.backButton.setText(QtGui.QApplication.translate("HistoryDialog", "Back", None, QtGui.QApplication.UnicodeUTF8))
    self.nextButton.setText(QtGui.QApplication.translate("HistoryDialog", "Next", None, QtGui.QApplication.UnicodeUTF8))
    self.closeButton.setText(QtGui.QApplication.translate("HistoryDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc

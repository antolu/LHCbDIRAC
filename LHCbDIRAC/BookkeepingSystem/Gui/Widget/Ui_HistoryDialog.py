# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'HistoryDialog.ui'
#
# Created: Fri Sep  7 13:24:22 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_HistoryDialog(object):
  def setupUi(self, HistoryDialog):
    HistoryDialog.setObjectName("HistoryDialog")
    HistoryDialog.resize(909, 498)
    self.gridLayout = QtGui.QGridLayout(HistoryDialog)
    self.gridLayout.setObjectName("gridLayout")
    self.backButton = QtGui.QPushButton(HistoryDialog)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/back.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.backButton.setIcon(icon)
    self.backButton.setObjectName("backButton")
    self.gridLayout.addWidget(self.backButton, 1, 0, 1, 1)
    self.nextButton = QtGui.QPushButton(HistoryDialog)
    self.nextButton.setEnabled(True)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(":/icons/images/next.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.nextButton.setIcon(icon1)
    self.nextButton.setObjectName("nextButton")
    self.gridLayout.addWidget(self.nextButton, 1, 1, 1, 1)
    self.closeButton = QtGui.QPushButton(HistoryDialog)
    icon2 = QtGui.QIcon()
    icon2.addPixmap(QtGui.QPixmap(":/icons/images/close.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.closeButton.setIcon(icon2)
    self.closeButton.setObjectName("closeButton")
    self.gridLayout.addWidget(self.closeButton, 1, 2, 1, 1)
    self.groupBox = QtGui.QGroupBox(HistoryDialog)
    self.groupBox.setObjectName("groupBox")
    self.gridLayout_3 = QtGui.QGridLayout(self.groupBox)
    self.gridLayout_3.setObjectName("gridLayout_3")
    self.jobTableView = QtGui.QTableView(self.groupBox)
    self.jobTableView.setObjectName("jobTableView")
    self.gridLayout_3.addWidget(self.jobTableView, 1, 1, 1, 1)
    self.groupBox_2 = QtGui.QGroupBox(self.groupBox)
    self.groupBox_2.setObjectName("groupBox_2")
    self.gridLayout_2 = QtGui.QGridLayout(self.groupBox_2)
    self.gridLayout_2.setObjectName("gridLayout_2")
    self.filesTableView = QtGui.QTableView(self.groupBox_2)
    self.filesTableView.setObjectName("filesTableView")
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

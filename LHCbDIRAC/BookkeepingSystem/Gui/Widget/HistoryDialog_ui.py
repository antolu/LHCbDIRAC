# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'HistoryDialog.ui'
#
# Created: Thu Mar 25 12:02:02 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_HistoryDialog(object):
    def setupUi(self, HistoryDialog):
        HistoryDialog.setObjectName("HistoryDialog")
        HistoryDialog.resize(QtCore.QSize(QtCore.QRect(0,0,909,498).size()).expandedTo(HistoryDialog.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(HistoryDialog)
        self.gridlayout.setObjectName("gridlayout")

        self.backButton = QtGui.QPushButton(HistoryDialog)
        self.backButton.setObjectName("backButton")
        self.gridlayout.addWidget(self.backButton,1,0,1,1)

        self.nextButton = QtGui.QPushButton(HistoryDialog)
        self.nextButton.setEnabled(True)
        self.nextButton.setObjectName("nextButton")
        self.gridlayout.addWidget(self.nextButton,1,1,1,1)

        self.closeButton = QtGui.QPushButton(HistoryDialog)
        self.closeButton.setObjectName("closeButton")
        self.gridlayout.addWidget(self.closeButton,1,2,1,1)

        self.groupBox = QtGui.QGroupBox(HistoryDialog)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout1.setObjectName("gridlayout1")

        self.jobTableView = QtGui.QTableView(self.groupBox)
        self.jobTableView.setObjectName("jobTableView")
        self.gridlayout1.addWidget(self.jobTableView,1,1,1,1)

        self.groupBox_2 = QtGui.QGroupBox(self.groupBox)
        self.groupBox_2.setObjectName("groupBox_2")

        self.gridlayout2 = QtGui.QGridLayout(self.groupBox_2)
        self.gridlayout2.setObjectName("gridlayout2")

        self.filesTableView = QtGui.QTableView(self.groupBox_2)
        self.filesTableView.setObjectName("filesTableView")
        self.gridlayout2.addWidget(self.filesTableView,0,0,1,1)
        self.gridlayout1.addWidget(self.groupBox_2,1,0,1,1)
        self.gridlayout.addWidget(self.groupBox,0,0,1,3)

        self.retranslateUi(HistoryDialog)
        QtCore.QMetaObject.connectSlotsByName(HistoryDialog)

    def retranslateUi(self, HistoryDialog):
        HistoryDialog.setWindowTitle(QtGui.QApplication.translate("HistoryDialog", "Feicim File History dialog window", None, QtGui.QApplication.UnicodeUTF8))
        self.backButton.setText(QtGui.QApplication.translate("HistoryDialog", "Back", None, QtGui.QApplication.UnicodeUTF8))
        self.nextButton.setText(QtGui.QApplication.translate("HistoryDialog", "Next", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("HistoryDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))


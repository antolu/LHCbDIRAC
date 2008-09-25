########################################################################
# $Id: InfoDialog_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'workspace/BookkeepingSystem/Bookkeeping/Widget/InfoDialog.ui'
#
# Created: Tue Sep  9 16:28:52 2008
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

__RCSID__ = "$Id: InfoDialog_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(QtCore.QSize(QtCore.QRect(0,0,505,310).size()).expandedTo(Dialog.minimumSizeHint()))

        self.pushButton = QtGui.QPushButton(Dialog)
        self.pushButton.setGeometry(QtCore.QRect(380,260,111,31))
        self.pushButton.setObjectName("pushButton")

        self.tableView = QtGui.QTableView(Dialog)
        self.tableView.setGeometry(QtCore.QRect(10,30,491,221))
        self.tableView.setObjectName("tableView")

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QtGui.QApplication.translate("Dialog", "Node Informations", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButton.setText(QtGui.QApplication.translate("Dialog", "Close", None, QtGui.QApplication.UnicodeUTF8))


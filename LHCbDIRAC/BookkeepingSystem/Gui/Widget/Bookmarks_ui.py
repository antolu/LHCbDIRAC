# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'Bookmarks.ui'
#
# Created: Thu Feb  4 13:09:27 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_BookmarksWidget(object):
    def setupUi(self, BookmarksWidget):
        BookmarksWidget.setObjectName("BookmarksWidget")
        BookmarksWidget.resize(QtCore.QSize(QtCore.QRect(0,0,508,794).size()).expandedTo(BookmarksWidget.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(BookmarksWidget)
        self.gridlayout.setObjectName("gridlayout")

        self.groupBox_3 = QtGui.QGroupBox(BookmarksWidget)
        self.groupBox_3.setObjectName("groupBox_3")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox_3)
        self.gridlayout1.setObjectName("gridlayout1")

        self.groupBox_2 = QtGui.QGroupBox(self.groupBox_3)
        self.groupBox_2.setObjectName("groupBox_2")

        self.gridlayout2 = QtGui.QGridLayout(self.groupBox_2)
        self.gridlayout2.setObjectName("gridlayout2")

        self.lineEdit = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout2.addWidget(self.lineEdit,1,0,1,2)
        self.gridlayout1.addWidget(self.groupBox_2,1,1,1,1)

        self.treeWidget = QtGui.QTreeWidget(self.groupBox_3)
        self.treeWidget.setObjectName("treeWidget")
        self.gridlayout1.addWidget(self.treeWidget,2,1,1,1)

        self.groupBox = QtGui.QGroupBox(self.groupBox_3)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout3 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout3.setObjectName("gridlayout3")

        spacerItem = QtGui.QSpacerItem(40,20,QtGui.QSizePolicy.Expanding,QtGui.QSizePolicy.Minimum)
        self.gridlayout3.addItem(spacerItem,0,0,1,1)

        self.removeButton = QtGui.QPushButton(self.groupBox)
        self.removeButton.setObjectName("removeButton")
        self.gridlayout3.addWidget(self.removeButton,0,1,1,1)

        self.addButton = QtGui.QPushButton(self.groupBox)
        self.addButton.setObjectName("addButton")
        self.gridlayout3.addWidget(self.addButton,0,2,1,1)
        self.gridlayout1.addWidget(self.groupBox,3,0,1,2)
        self.gridlayout.addWidget(self.groupBox_3,1,0,1,1)

        self.closeButton = QtGui.QToolButton(BookmarksWidget)
        self.closeButton.setObjectName("closeButton")
        self.gridlayout.addWidget(self.closeButton,0,0,1,1)

        self.retranslateUi(BookmarksWidget)
        QtCore.QMetaObject.connectSlotsByName(BookmarksWidget)

    def retranslateUi(self, BookmarksWidget):
        BookmarksWidget.setWindowTitle(QtGui.QApplication.translate("BookmarksWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.treeWidget.headerItem().setText(0,QtGui.QApplication.translate("BookmarksWidget", "BookMarks", None, QtGui.QApplication.UnicodeUTF8))
        self.removeButton.setText(QtGui.QApplication.translate("BookmarksWidget", "Remove", None, QtGui.QApplication.UnicodeUTF8))
        self.addButton.setText(QtGui.QApplication.translate("BookmarksWidget", "Add", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("BookmarksWidget", "...", None, QtGui.QApplication.UnicodeUTF8))


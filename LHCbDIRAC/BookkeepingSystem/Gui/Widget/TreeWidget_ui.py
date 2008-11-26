# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TreeWidget.ui'
#
# Created: Tue Nov 25 16:19:48 2008
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui
from DIRAC.BookkeepingSystem.Gui.Widget.TreePanel    import TreePanel

class Ui_TreeWidget(object):
    def setupUi(self, TreeWidget):
        TreeWidget.setObjectName("TreeWidget")
        TreeWidget.resize(QtCore.QSize(QtCore.QRect(0,0,584,775).size()).expandedTo(TreeWidget.minimumSizeHint()))

        self.selection = QtGui.QGroupBox(TreeWidget)
        self.selection.setGeometry(QtCore.QRect(10,650,561,101))
        self.selection.setObjectName("selection")

        self.radioButton_2 = QtGui.QRadioButton(self.selection)
        self.radioButton_2.setGeometry(QtCore.QRect(0,60,421,23))
        self.radioButton_2.setObjectName("radioButton_2")

        self.configNameRadioButton = QtGui.QRadioButton(self.selection)
        self.configNameRadioButton.setGeometry(QtCore.QRect(0,20,411,23))
        self.configNameRadioButton.setObjectName("configNameRadioButton")

        self.tree = TreePanel(TreeWidget)
        self.tree.setWindowModality(QtCore.Qt.WindowModal)
        self.tree.setEnabled(True)
        self.tree.setGeometry(QtCore.QRect(10,20,561,621))
        self.tree.setProperty("cursor",QtCore.QVariant(QtCore.Qt.ArrowCursor))
        self.tree.setLineWidth(1)
        self.tree.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.tree.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.tree.setTabKeyNavigation(True)
        self.tree.setTextElideMode(QtCore.Qt.ElideNone)
        self.tree.setSortingEnabled(True)
        self.tree.setAnimated(False)
        self.tree.setObjectName("tree")

        self.configNameRadioButton1 = QtGui.QAction(TreeWidget)
        self.configNameRadioButton1.setObjectName("configNameRadioButton1")

        self.retranslateUi(TreeWidget)
        QtCore.QMetaObject.connectSlotsByName(TreeWidget)

    def retranslateUi(self, TreeWidget):
        TreeWidget.setWindowTitle(QtGui.QApplication.translate("TreeWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.selection.setTitle(QtGui.QApplication.translate("TreeWidget", "Queries", None, QtGui.QApplication.UnicodeUTF8))
        self.radioButton_2.setText(QtGui.QApplication.translate("TreeWidget", "Event type/SimCond/ProcessingPass/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
        self.configNameRadioButton.setText(QtGui.QApplication.translate("TreeWidget", "SimCond/ProcessingPass/Eventtype/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
        self.tree.headerItem().setText(0,QtGui.QApplication.translate("TreeWidget", "                                Tree                         ", None, QtGui.QApplication.UnicodeUTF8))
        self.tree.headerItem().setText(1,QtGui.QApplication.translate("TreeWidget", "Description", None, QtGui.QApplication.UnicodeUTF8))
        self.configNameRadioButton1.setText(QtGui.QApplication.translate("TreeWidget", "config_click", None, QtGui.QApplication.UnicodeUTF8))


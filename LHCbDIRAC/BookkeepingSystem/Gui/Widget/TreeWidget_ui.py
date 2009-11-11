# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TreeWidget.ui'
#
# Created: Mon Jun 22 18:01:07 2009
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreePanel    import TreePanel

class Ui_TreeWidget(object):
    def setupUi(self, TreeWidget):
        TreeWidget.setObjectName("TreeWidget")
        TreeWidget.resize(QtCore.QSize(QtCore.QRect(0,0,456,736).size()).expandedTo(TreeWidget.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(TreeWidget)
        self.gridlayout.setObjectName("gridlayout")

        self.groupBox = QtGui.QGroupBox(TreeWidget)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout1.setObjectName("gridlayout1")

        self.standardQuery = QtGui.QCheckBox(self.groupBox)
        self.standardQuery.setObjectName("standardQuery")
        self.gridlayout1.addWidget(self.standardQuery,0,0,1,1)

        self.advancedQuery = QtGui.QCheckBox(self.groupBox)
        self.advancedQuery.setObjectName("advancedQuery")
        self.gridlayout1.addWidget(self.advancedQuery,0,1,1,1)

        self.pageSize = QtGui.QLineEdit(self.groupBox)
        self.pageSize.setObjectName("pageSize")
        self.gridlayout1.addWidget(self.pageSize,2,1,1,1)

        self.label = QtGui.QLabel(self.groupBox)
        self.label.setObjectName("label")
        self.gridlayout1.addWidget(self.label,2,0,1,1)

        spacerItem = QtGui.QSpacerItem(40,20,QtGui.QSizePolicy.Expanding,QtGui.QSizePolicy.Minimum)
        self.gridlayout1.addItem(spacerItem,2,2,1,1)
        self.gridlayout.addWidget(self.groupBox,0,0,1,1)

        self.tree = TreePanel(TreeWidget)
        self.tree.setEnabled(True)
        self.tree.setProperty("cursor",QtCore.QVariant(QtCore.Qt.ArrowCursor))
        self.tree.setLineWidth(1)
        self.tree.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.tree.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
        self.tree.setTabKeyNavigation(True)
        self.tree.setTextElideMode(QtCore.Qt.ElideNone)
        self.tree.setSortingEnabled(False)
        self.tree.setAnimated(False)
        self.tree.setObjectName("tree")
        self.gridlayout.addWidget(self.tree,1,0,1,1)

        self.selection = QtGui.QGroupBox(TreeWidget)
        self.selection.setObjectName("selection")

        self.gridlayout2 = QtGui.QGridLayout(self.selection)
        self.gridlayout2.setObjectName("gridlayout2")

        self.configNameRadioButton = QtGui.QRadioButton(self.selection)
        self.configNameRadioButton.setObjectName("configNameRadioButton")
        self.gridlayout2.addWidget(self.configNameRadioButton,0,0,1,1)

        self.radioButton_2 = QtGui.QRadioButton(self.selection)
        self.radioButton_2.setObjectName("radioButton_2")
        self.gridlayout2.addWidget(self.radioButton_2,1,0,1,1)

        self.productionRadioButton = QtGui.QRadioButton(self.selection)
        self.productionRadioButton.setObjectName("productionRadioButton")
        self.gridlayout2.addWidget(self.productionRadioButton,2,0,1,1)

        self.runLookup = QtGui.QRadioButton(self.selection)
        self.runLookup.setObjectName("runLookup")
        self.gridlayout2.addWidget(self.runLookup,3,0,1,1)
        self.gridlayout.addWidget(self.selection,2,0,1,1)

        self.configNameRadioButton1 = QtGui.QAction(TreeWidget)
        self.configNameRadioButton1.setObjectName("configNameRadioButton1")

        self.retranslateUi(TreeWidget)
        QtCore.QMetaObject.connectSlotsByName(TreeWidget)

    def retranslateUi(self, TreeWidget):
        TreeWidget.setWindowTitle(QtGui.QApplication.translate("TreeWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
        self.standardQuery.setText(QtGui.QApplication.translate("TreeWidget", "Standard", None, QtGui.QApplication.UnicodeUTF8))
        self.advancedQuery.setText(QtGui.QApplication.translate("TreeWidget", "Advanced Queries", None, QtGui.QApplication.UnicodeUTF8))
        self.pageSize.setText(QtGui.QApplication.translate("TreeWidget", "ALL", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("TreeWidget", "Page Size:", None, QtGui.QApplication.UnicodeUTF8))
        self.tree.headerItem().setText(0,QtGui.QApplication.translate("TreeWidget", "                                Tree                                                     ", None, QtGui.QApplication.UnicodeUTF8))
        self.tree.headerItem().setText(1,QtGui.QApplication.translate("TreeWidget", "Description", None, QtGui.QApplication.UnicodeUTF8))
        self.selection.setTitle(QtGui.QApplication.translate("TreeWidget", "Queries", None, QtGui.QApplication.UnicodeUTF8))
        self.configNameRadioButton.setText(QtGui.QApplication.translate("TreeWidget", "SimCond/ProcessingPass/Eventtype/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
        self.radioButton_2.setText(QtGui.QApplication.translate("TreeWidget", "Event type/SimCond/ProcessingPass/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
        self.productionRadioButton.setText(QtGui.QApplication.translate("TreeWidget", "Production lookup", None, QtGui.QApplication.UnicodeUTF8))
        self.runLookup.setText(QtGui.QApplication.translate("TreeWidget", "Run lookup", None, QtGui.QApplication.UnicodeUTF8))
        self.configNameRadioButton1.setText(QtGui.QApplication.translate("TreeWidget", "config_click", None, QtGui.QApplication.UnicodeUTF8))


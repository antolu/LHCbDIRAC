# pylint: skip-file

"""
This widget used to view a key/value pair dataset
"""

from PyQt4.QtGui                                import QDialog
from PyQt4.QtCore                               import SIGNAL
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_InfoDialog           import Ui_InfoDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerInfoDialog  import ControlerInfoDialog

__RCSID__ = "$Id$"

#############################################################################
class InfoDialog(QDialog, Ui_InfoDialog):
  """
  InfoDialog class
  """
  #############################################################################
  def __init__(self, parent=None):
    """initialize the widget"""
    QDialog.__init__(self, parent)
    Ui_InfoDialog.__init__(self)
    self.setupUi(self)
    self.__controler = ControlerInfoDialog(self, parent.getControler())
    self.connect(self.pushButton, SIGNAL("clicked()"), self.__controler.close)

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def showData(self, data):
    """hows the data"""
    noheader = ['name', 'expandable', 'level', 'fullpath']
    tabledata = []
    header = ['Name', 'Value']

    for item in data.keys():
      if item not in noheader:
        if data[item] == None:
          i = ''
        else:
          i = data[item]
        tabledata += [ [item, i] ]

    if len(tabledata) > 0:
      self.filltable(header, tabledata)
      return True

  #############################################################################
  def showDictionary(self, data):
    """shows the dictionary content"""
    header = ['FileName', 'Ancestor1', 'Ancestor2', 'Ancestor3', 'Ancestor4', 'Ancestor5', 'Ancestor6']
    keys = data.keys()
    keys.sort()
    tabledata = []
    for i in keys:
      j = data[i]
      if len(j) == 0:
        tabledata += [ [i, '', '', '', '', '', '']]
      else:
        tmp = ['', '', '', '', '', '', '']
        k = 1
        tmp[0] = i
        for an in j:
          tmp[k] = an
          k += 1
        tabledata += [tmp]
    if len(tabledata) > 0:
      self.filltable(header, tabledata)
    return True

  #############################################################################
  def filltable(self, header, tabledata):
    """fills the table"""
    # set the table model
    tm = TableModel(tabledata, header, self)
    self.tableView.setModel(tm)

    # set the minimum size
    self.setMinimumSize(400, 300)

    # hide grid
    self.tableView.setShowGrid(False)

    # set the font
    #font = QFont("Courier New", 8)
    #self.tableView.setFont(font)

    self.tableView.setSortingEnabled(True)

    # hide vertical header
    vh = self.tableView.verticalHeader()
    vh.setVisible(False)

    # set horizontal header properties
    hh = self.tableView.horizontalHeader()
    hh.setStretchLastSection(True)

    # set column width to fit contents
    self.tableView.resizeColumnsToContents()

    # set row height
    nrows = len(tabledata)
    for row in xrange(nrows):
      self.tableView.setRowHeight(row, 18)

    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)


#############################################################################

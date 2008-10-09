########################################################################
# $Id: FileDialog.py,v 1.2 2008/10/09 13:50:44 zmathe Exp $
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *
from DIRAC.BookkeepingSystem.Gui.Widget.FileDialog_ui           import Ui_FileDialog
from DIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerFileDialog  import ControlerFileDialog
import os

__RCSID__ = "$Id: FileDialog.py,v 1.2 2008/10/09 13:50:44 zmathe Exp $"

#############################################################################  
class FileDialog(QDialog, Ui_FileDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerFileDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.save)   
    
    self.__model = None
    self.__fileExtension = None
    self.__popUp = QMenu(self.tableView)
    
    self.__jobAction = QAction(self.tr("Job Info"), self.tableView)
    self.connect (self.__jobAction, SIGNAL("triggered()"), self.__controler.jobinfo)
    self.__popUp.addAction(self.__jobAction)
    
    self.__closeAction = QAction(self.tr("Close"), self.tableView)
    self.connect (self.__closeAction, SIGNAL("triggered()"), self.__controler.close)
    self.__popUp.addAction(self.__closeAction)
    
    self.tableView.setContextMenuPolicy(Qt.CustomContextMenu);
    self.connect(self.tableView, SIGNAL('customContextMenuRequested(QPoint)'), self.popUpMenu)


  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def setModel(self, model):
    self.__model = model
  
  #############################################################################  
  def getModel(self):
    return self.__model
  
  #############################################################################  
  def showNumberOfEvents(self, number):
    self.lineEdit_2.setText(str(number))
  
  #############################################################################  
  def showNumberOfFiles(self, number):
    self.lineEdit.setText(str(number))
  
  #############################################################################  
  def showFilesSize(self, number):
    self.lineEdit_5.setText(str(number)+'  GB')
  
  #############################################################################  
  def showSelectedNumberOfEvents(self, number):
    self.lineEdit_4.setText(str(number))
  
  #############################################################################  
  def showSelectedNumberOfFiles(self, number):
    self.lineEdit_3.setText(str(number))
  
  #############################################################################  
  def showSelectedFileSize(self, number):
    self.lineEdit_6.setText(str(number)+'  GB')
  
  #############################################################################  
  def showData(self, data):
    noheader = ['name','expandable','level','fullpath', 'GeometryVersion','WorkerNode']
    tabledata =[]
    header = ['FileName','EventStat', 'FileSize', 'FileType','CreationDate','Generator','JobStart', 'JobEnd', 'EvtTypeId']
    keys = data.keys()
    keys.sort()
    for item in keys:
      file = data[item]
      d = []
      '''
      for info in file:
        if info not in noheader:
          header += [info]
          value = file[info]
          if value == None:
            value = ''
          d += [value]
      '''
      for info in header:
          value = file[info]
          if value == None:
            value = ''
          d += [value]
      tabledata += [d]
          
    if len(tabledata) > 0:
      self.filltable(header, tabledata)
      return True
  
  #############################################################################  
  def filltable(self, header, tabledata):
      
    # set the table model
    tm = TableModel(tabledata, header, self) 
    self.tableView.setModel(tm)
    self.tableView.setSelectionBehavior(QAbstractItemView.SelectRows)
    self.tableView.setAlternatingRowColors(True)

    sm = self.tableView.selectionModel()
    self.connect(sm, SIGNAL("selectionChanged(QItemSelection, QItemSelection)"), self.__controler.selection)
    
    # set the minimum size
    self.setMinimumSize(400, 300)
  
    # hide grid
    self.tableView.setShowGrid(True)
  
    # set the font
    #font = QFont("Courier New", 12)
    #self.tableView.setFont(font)
  
    # hide vertical header
    vh = self.tableView.verticalHeader()
    vh.setVisible(False)
  
    # set horizontal header properties
    hh = self.tableView.horizontalHeader()
    hh.setStretchLastSection(True)
  
    # set column width to fit contents
    self.tableView.resizeColumnsToContents()
    self.tableView.setSortingEnabled(True)
  
    # set row height
    nrows = len(tabledata)
    for row in xrange(nrows):
        self.tableView.setRowHeight(row, 18)
  
    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)
  
  #############################################################################  
  def saveAs(self):
    
    saveDialog = QFileDialog (self,'Save files')
    filename = ''
    #self.connect(saveDialog, SIGNAL("filterSelected(const QString &)"),self.filter ) 
    saveDialog.setDirectory (QDir.currentPath())
    #filters = ['Option file (*.opts)' ,'Pool xml file (*.xml)','*.txt']    
    filters = ['Option file (*.opts)','*.py','*.txt']    
    saveDialog.setFilter(';;'.join(filters))
    saveDialog.setFileMode(QFileDialog.AnyFile)
    saveDialog.setViewMode(QFileDialog.Detail)
   
    if (saveDialog.exec_()):
      filename = str(saveDialog.selectedFiles()[0]) 
      ext = saveDialog.selectedFilter()
      filename += ext[1:]
      try:
        open(filename)
      except IOError:
        pass
      else:
        v = QMessageBox.warning(self, "File dialog", "File exists, overwrite?",QMessageBox.Ok,QMessageBox.No)
        if v == QMessageBox.No:
          filename = ''
    if filename =='':
      return '',''
    
    return filename,ext
  
  #############################################################################  
  def popUpMenu(self):
    self.__popUp.popup(QCursor.pos())


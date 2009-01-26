########################################################################
# $Id: FileDialog.py,v 1.10 2009/01/26 17:38:01 zmathe Exp $
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *
from DIRAC.BookkeepingSystem.Gui.Widget.FileDialog_ui           import Ui_FileDialog
from DIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerFileDialog  import ControlerFileDialog
import DIRAC,os

__RCSID__ = "$Id: FileDialog.py,v 1.10 2009/01/26 17:38:01 zmathe Exp $"

#############################################################################  
class FileDialog(QDialog, Ui_FileDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerFileDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.save)   
    
    picturesPath = DIRAC.rootPath+'/DIRAC/BookkeepingSystem/Gui/Widget'
    saveIcon = QIcon(picturesPath+"/images/save.png")
    self.saveButton.setIcon(saveIcon)
    
    closeIcon = QIcon(picturesPath+"/images/close.png")
    self.closeButton.setIcon(closeIcon)
    self.__model = None
    self.__fileExtension = None
    self.__popUp = QMenu(self.tableView)
    
    self.__jobAction = QAction(self.tr("Job Info"), self.tableView)
    self.connect (self.__jobAction, SIGNAL("triggered()"), self.__controler.jobinfo)
    self.__popUp.addAction(self.__jobAction)
    
    self.__ancesstorsAction = QAction(self.tr("Get Anccestors"), self.tableView)
    self.connect (self.__ancesstorsAction, SIGNAL("triggered()"), self.__controler.getancesstots)
    self.__popUp.addAction(self.__ancesstorsAction)
    
    
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
  def showError(self, message):
    QMessageBox.critical(self, "ERROR", message,QMessageBox.Ok)
  
  #############################################################################  
  def showData(self, data):
    noheader = ['name','expandable','level','fullpath', 'GeometryVersion','WorkerNode', 'FileType','EvtTypeId']
    tabledata =[]
    header = ['FileName','EventStat', 'FileSize', 'CreationDate','Generator','JobStart', 'JobEnd', 'RunNumber','FillNumber','PhysicStat']
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
    self.tableView.setSelectionMode(QAbstractItemView.MultiSelection)
  
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
    vh.setVisible(True)
  
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
    
    saveDialog = QFileDialog (self,'Feicim Save file(s) dialog',QDir.currentPath(),'Python option(*.py);;Option file (*.opts);;Text file (*.txt)')
    filename = ''
    #self.connect(saveDialog, SIGNAL("filterSelected(const QString &)"),self.filter ) 
    ##saveDialog.setDirectory (QDir.currentPath())
    #filters = ['Option file (*.opts)' ,'Pool xml file (*.xml)','*.txt']    
    #filters = ['Option file (*.opts)','*.py','*.txt']    
    #saveDialog.setFilter(';;'.join(filters))
    #saveDialog.setFilter('Option file (*.opts);;Text file (*.txt);;Python option')
    #saveDialog.setFileMode(QFileDialog.AnyFile)
    #saveDialog.setViewMode(QFileDialog.Detail)
   
    if (saveDialog.exec_()):
      filename = str(saveDialog.selectedFiles()[0]) 
      ext = saveDialog.selectedFilter()
      if 'Text file (*.txt)' in ext:
        filename += '.txt'
      elif 'Option file (*.opts)' in ext:
        filename += '.opts'
      elif 'Python option(*.py)' in ext:  
        filename += '.py'
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
  
  #############################################################################  
  def showSelection(self, dict):
    self.configname.setText(dict["Configuration Name"])
    self.configversion.setText(dict["Configuration Version"])
    self.simulation.setText(dict["Simulation Condition"])
    self.processing.setText(dict["Processing Pass"])
    self.eventtype.setText(dict["Event type"])
    self.filetype.setText(dict["File Type"])
    self.production.setText(dict["Production"])
    self.progrnameandversion.setText(dict["Program name"] + ' - '+dict["Program version"])
    

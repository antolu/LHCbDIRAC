########################################################################
# $Id$
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FileDialog_ui           import Ui_FileDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.LogFileWidget           import LogFileWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.AdvancedSave            import AdvancedSave
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerFileDialog  import ControlerFileDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.HistoryDialog           import HistoryDialog
import LHCbDIRAC,os

__RCSID__ = "$Id$"

#############################################################################  
class FileDialog(QDialog, Ui_FileDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    flags = 0
    flags = Qt.Window | Qt.WindowMinimizeButtonHint;
    self.setWindowFlags( flags )
    self.__controler = ControlerFileDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.save)   
    self.connect(self.advancedSave, SIGNAL("clicked()"), self.__controler.advancedSave)
    self.connect(self.nextButton, SIGNAL("clicked()"), self.__controler.next)
    
    picturesPath = os.path.dirname(os.path.realpath(LHCbDIRAC.__path__[0]))+'/LHCbDIRAC/BookkeepingSystem/Gui/Widget'
    saveIcon = QIcon(picturesPath+"/images/save.png")
    self.saveButton.setIcon(saveIcon)
    self.advancedSave.setIcon(saveIcon)
    
    closeIcon = QIcon(picturesPath+"/images/close.png")
    self.closeButton.setIcon(closeIcon)
    self.__model = {}
    self.__path = None
    self.__fileExtension = None
    self.__popUp = QMenu(self.tableView)
    
    self.__jobAction = QAction(self.tr("Job Info"), self.tableView)
    self.connect (self.__jobAction, SIGNAL("triggered()"), self.__controler.jobinfo)
    self.__popUp.addAction(self.__jobAction)
    
    self.__ancesstorsAction = QAction(self.tr("Get Anccestors"), self.tableView)
    self.connect (self.__ancesstorsAction, SIGNAL("triggered()"), self.__controler.getancesstots)
    self.__popUp.addAction(self.__ancesstorsAction)
    
    self.__loginfoAction = QAction(self.tr("Logginig informations"), self.tableView)
    self.connect (self.__loginfoAction, SIGNAL("triggered()"), self.__controler.loggininginfo)
    self.__popUp.addAction(self.__loginfoAction)
    
    self.tableView.setContextMenuPolicy(Qt.CustomContextMenu);
    self.connect(self.tableView, SIGNAL('customContextMenuRequested(QPoint)'), self.popUpMenu)

    self.__log = LogFileWidget(self)
    self.__controler.addChild('LogFileWidget',self.__log.getControler())
    
    self.__advancedSave = AdvancedSave(self)
    self.__controler.addChild('AdvancedSave', self.__advancedSave.getControler())
    
    self.__historyDialog = HistoryDialog(self)
    self.__controler.addChild('HistoryDialog',self.__historyDialog.getControler())
    
  #############################################################################
  def closeEvent (self, event ):
    self.getControler().close()
     
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def setModel(self, model):
    self.__model = model
  
  def updateModel(self, model):
    self.__model.update(model)
    
  #############################################################################  
  def getModel(self):
    return self.__model
  
  #############################################################################  
  def setPath(self, path):
    self.__path = path
  
  #############################################################################  
  def getPath(self):
    return self.__path
  
  #############################################################################  
  def showNumberOfEvents(self, number):
    self.lineEdit_2.setText(str(number))
  
  #############################################################################  
  def showNumberOfFiles(self, number):
    self.lineEdit.setText(str(number))
  
  #############################################################################
  def showEventInputStat(self, number):
    self.alleventinputstat.setText(str(number))
  
  #############################################################################  
  def showFilesSize(self, number):
    self.lineEdit_5.setText(str(number)+'  GB')
  
  #############################################################################  
  def showSelectedNumberOfEvents(self, number):
    self.lineEdit_4.setText(str(number))
  
  #############################################################################
  def showSelectedEventInputStat(self, number):
    self.eventInputstat.setText(str(number))
  
  #############################################################################  
  def showSelectedNumberOfFiles(self, number):
    self.lineEdit_3.setText(str(number))
  
  #############################################################################  
  def showSelectedFileSize(self, number):
    self.lineEdit_6.setText(str(number)+'  GB')
  
  #############################################################################
  def showTotalLuminosity(self, number):
    self.alltotalluminosity.setText(str(number))
    
  #############################################################################
  def showSelectedTotalLuminosity(self, number):
    self.totalluminosity.setText(str(number))
  
  #############################################################################
  def showLuminosity(self, number):
    self.allluminosity.setText(str(number))
  
  #############################################################################
  def showSelectedLuminosity(self, number):
    self.luminosity.setText(str(number))
  
  #############################################################################  
  def showError(self, message):
    QMessageBox.critical(self, "ERROR", message,QMessageBox.Ok)
  
  #############################################################################  
  def showData(self, data):
    noheader = ['name','expandable','level','fullpath', 'GeometryVersion','WorkerNode', 'FileType','EvtTypeId', 'Generator']
    tabledata =[]
    #print data
             #['Name','EventStat', 'FileSize','CreationDate', 'JobStart', 'JobEnd','WorkerNode','FileType', 'EvtTypeId','RunNumber','FillNumber','FullStat', 'DataQuality', 'EventInputStat']  
    header = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd', 'WorkerNode', 'RunNumber', 'FillNumber', 'FullStat', 'DataqualityFlag',
    'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity']
    data.update(self.__model)
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
    self.tableView.setSelectionMode(QAbstractItemView.ExtendedSelection) 
  
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
  def saveAs(self, filename = ''):
    
    saveDialog = QFileDialog (self,'Feicim Save file(s) dialog',QDir.currentPath(),'Python option(*.py);;Option file (*.opts);;Text file (*.txt)')
    saveDialog.setAcceptMode(QFileDialog.AcceptSave)
    
    saveDialog.selectFile(filename)
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
        if filename.find('.') < 0:
          filename += '.txt'
      elif 'Option file (*.opts)' in ext:
        if filename.find('.') < 0:
          filename += '.opts'
      elif 'Python option(*.py)' in ext:  
        if filename.find('.') < 0:
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
    
    
    if dict.has_key('ConfigName'):
      self.configname.setText(dict["ConfigName"])
      
    if dict.has_key('ConfigVersion'):
      self.configversion.setText(dict["ConfigVersion"])
    
    if dict.has_key('ConditionDescription'):
      self.simulation.setText(dict["ConditionDescription"])
    
    if dict.has_key('ProcessingPass'):
      self.processing.setText(dict["ProcessingPass"])
    
    if dict.has_key('EventTypeId'):
      self.eventtype.setText(dict["EventTypeId"])
    
    if dict.has_key('Production'):
      self.production.setText('')
    
    if dict.has_key('FileType'):
      self.filetype.setText(dict["FileType"])
    
    self.progrnameandversion.setText('')
  
  def clearTable(self):
    #self.tableView().clear()
    self.__model = {}
    

# pylint: skip-file

"""
File dialog widget
"""
########################################################################
# $Id: FileDialog.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################

from PyQt4.QtGui                                import QDialog, QMenu, QAction, \
                                                       QSortFilterProxyModel, QMessageBox, \
                                                       QAbstractItemView, QFileDialog, QCursor
from PyQt4.QtCore                               import SIGNAL, Qt, QDir, QVariant

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_FileDialog           import Ui_FileDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.LogFileWidget           import LogFileWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.AdvancedSave            import AdvancedSave
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerFileDialog  import ControlerFileDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.HistoryDialog           import HistoryDialog
from DIRAC                                                          import gLogger

__RCSID__ = "$Id$"

#############################################################################
class FileDialog(QDialog, Ui_FileDialog):
  """
  FileDialog class
  """
  #############################################################################
  def __init__(self, parent=None):
    """
    initialize the widget
    """
    QDialog.__init__(self, parent)
    Ui_FileDialog.__init__(self)
    self.setupUi(self)
    #flags = 0
    #flags = Qt.Window | Qt.WindowMinimizeButtonHint;
    #self.setWindowFlags( flags )
    self.hideTckFilter()
    self.__controler = ControlerFileDialog(self, parent.getControler())
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)
    self.connect(self.saveButton, SIGNAL("clicked()"), self.__controler.save)
    self.connect(self.advancedSave, SIGNAL("clicked()"), self.__controler.advancedSave)
    self.connect(self.nextButton, SIGNAL("clicked()"), self.__controler.next)

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

    self.__copyAction = QAction(self.tr("Copy data"), self.tableView)
    self.connect (self.__copyAction, SIGNAL("triggered()"), self.__controler.copy)
    self.__popUp.addAction(self.__copyAction)


    self.tableView.setContextMenuPolicy(Qt.CustomContextMenu)
    self.connect(self.tableView, SIGNAL('customContextMenuRequested(QPoint)'), self.popUpMenu)

    self.__log = LogFileWidget(self)
    self.__controler.addChild('LogFileWidget', self.__log.getControler())

    self.__advancedSave = AdvancedSave(self)
    self.__advancedSave.setFocus()

    self.__controler.addChild('AdvancedSave', self.__advancedSave.getControler())

    self.__historyDialog = HistoryDialog(self)
    self.__controler.addChild('HistoryDialog', self.__historyDialog.getControler())

    self.connect(self.tckcombo, SIGNAL('currentIndexChanged(QString)'), self.getControler().tckChanged)
    self.__proxy = QSortFilterProxyModel()

    self.connect(self.tckButton, SIGNAL("clicked()"), self.__controler.tckButtonPressed)
    self.connect(self.tckcloseButton, SIGNAL("clicked()"), self.__controler.hideFilterWidget)

    self.filterWidget.setupControler(self)
    self.__controler.addChild('TckFilterWidget', self.filterWidget.getControler())


  #############################################################################
  def closeEvent (self, event):
    """handles the close action"""
    gLogger.debug(event)
    self.getControler().close()

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def setModel(self, model):
    """sets the model"""
    self.__model = model

  def updateModel(self, model):
    """updates the model in case of change"""
    self.__model.update(model)

  #############################################################################
  def getModel(self):
    """returns the model"""
    return self.__model

  #############################################################################
  def setPath(self, path):
    """sets the path"""
    self.__path = path

  #############################################################################
  def getPath(self):
    """returns the path"""
    return self.__path

  #############################################################################
  def showNumberOfEvents(self, number):
    """shows the number of events"""
    self.lineEdit_2.setText(str(number))

  #############################################################################
  def showNumberOfFiles(self, number):
    """shows the number of files"""
    self.lineEdit.setText(str(number))

  #############################################################################
  def showEventInputStat(self, number):
    """shows the number of processed input events"""
    self.alleventinputstat.setText(str(number))

  #############################################################################
  def showFilesSize(self, number):
    """shows the size of the files"""
    self.lineEdit_5.setText(str(number) + '  GB')

  #############################################################################
  def showSelectedNumberOfEvents(self, number):
    """shows the selected number of events"""
    self.lineEdit_4.setText(str(number))

  #############################################################################
  def showSelectedEventInputStat(self, number):
    """shows the selected processed input events"""
    self.eventInputstat.setText(str(number))

  #############################################################################
  def showSelectedNumberOfFiles(self, number):
    """shoes the selected number of files"""
    self.lineEdit_3.setText(str(number))

  #############################################################################
  def showSelectedFileSize(self, number):
    """shows the selected file size"""
    self.lineEdit_6.setText(str(number) + '  GB')

  #############################################################################
  def showTotalLuminosity(self, number):
    """shows the total luminosity"""
    self.alltotalluminosity.setText(str(number))

  #############################################################################
  def showSelectedTotalLuminosity(self, number):
    """selected total luminosity"""
    self.totalluminosity.setText(str(number))

  #############################################################################
  def showLuminosity(self, number):
    """luminosity"""
    self.allluminosity.setText(str(number))

  #############################################################################
  def showSelectedLuminosity(self, number):
    """selected luminosity"""
    self.luminosity.setText(str(number))

  #############################################################################
  def showError(self, message):
    """shows the message as an ERROR"""
    QMessageBox.critical(self, "ERROR", message, QMessageBox.Ok)

  #############################################################################
  def showData(self, data):
    """shows the files in the table widget"""
    self.waitCursor()

    tabledata = []

    header = ['FileName', 'EventStat', 'FileSize', 'CreationDate', 'JobStart', 'JobEnd',
              'WorkerNode', 'RunNumber', 'FillNumber', 'FullStat', 'DataqualityFlag',
              'EventInputStat', 'TotalLuminosity', 'Luminosity', 'InstLuminosity', 'TCK']
    data.update(self.__model)
    keys = data.keys()
    keys.sort()
    for item in keys:
      lfn = data[item]
      i = []
      for info in header:
        value = lfn[info]
        if value == None:
          value = ''
        i += [value]
      tabledata += [i]

    if len(tabledata) > 0:
      self.filltable(header, tabledata)
    self.arrowCursor()
    return True

  #############################################################################
  def filltable(self, header, tabledata):
    """ fill the table widget"""
    # set the table model

    tm = TableModel(tabledata, header, self)

    self.__proxy.setSourceModel(tm)


    self.tableView.setModel(self.__proxy)
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

    self.__proxy.sort (0, Qt.AscendingOrder)
    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)

  #############################################################################
  def saveAs(self, filename=''):
    """saves the selected files"""
    saveDialog = QFileDialog (self, 'Feicim Save file(s) dialog',
                              QDir.currentPath(),
                              'Python option(*.py);;Option file (*.opts);;Text file (*.txt)')
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

    ext = ''
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
        response = QMessageBox.warning(self, "File dialog", "File exists, overwrite?", QMessageBox.Ok, QMessageBox.No)
        if response == QMessageBox.No:
          filename = ''
    if filename == '':
      return '', ''

    return filename, ext

  #############################################################################
  def popUpMenu(self):
    """shows the popup menu"""
    self.__popUp.popup(QCursor.pos())

  #############################################################################
  def showSelection(self, in_dict):
    """ shows the Bookkeeping query, the selected dataset"""

    if in_dict.has_key('ConfigName'):
      self.configname.setText(in_dict["ConfigName"])

    if in_dict.has_key('ConfigVersion'):
      self.configversion.setText(in_dict["ConfigVersion"])

    if in_dict.has_key('ConditionDescription'):
      self.simulation.setText(in_dict["ConditionDescription"])

    if in_dict.has_key('ProcessingPass'):
      self.processing.setText(in_dict["ProcessingPass"])

    if in_dict.has_key('EventTypeId'):
      self.eventtype.setText(in_dict["EventTypeId"])

    if in_dict.has_key('Production'):
      self.production.setText('')

    if in_dict.has_key('FileType'):
      self.filetype.setText(in_dict["FileType"])

    self.progrnameandversion.setText('')

  #############################################################################
  def clearTable(self):
    """clear the elements from the table"""
    #self.tableView().clear()
    self.__model = {}

  #############################################################################
  def fillTckFilter(self, data):
    """fills the tck combo box"""
    tcks = data + ['All']

    self.tckcombo.clear()
    j = 0
    for i in tcks:
      self.tckcombo.addItem (i, QVariant(i))
      if i == 'All':
        self.tckcombo.setCurrentIndex(j)
      j += 1
    #self.tckcombo.view().setSelectionMode(QAbstractItemView.MultiSelection)

  #############################################################################
  def applyFilter(self, data):
    """performs filter over the files"""
    if data == 'All':
      gLogger.debug('applyFilter-ALL')
      self.__proxy.clear()
      self.__proxy.invalidateFilter()
      filterCondition = "^\\S+$"
      gLogger.debug('Filter condition:'+filterCondition)
      self.__proxy.setFilterKeyColumn(15)
      self.__proxy.setFilterRegExp(filterCondition)
      for row in xrange(self.__proxy.rowCount()):
        self.tableView.setRowHeight(row, 18)
    else:
      gLogger.debug('applyFilter-Selected')
      self.__proxy.setFilterKeyColumn(15)
      filterCondition = '%s' % (data)
      gLogger.debug('Filter condition:'+filterCondition)
      self.__proxy.setFilterRegExp(filterCondition)
      for row in xrange(self.__proxy.rowCount()):
        self.tableView.setRowHeight(row, 18)

  def applyListFilter(self, data):
    """specific filter"""
    gLogger.debug('applyListFilter')
    filterCondition = '\\b'
    cond = '('
    for i in data:
      cond += i
      cond += '|'
    cond = cond[:-1]
    filterCondition += cond + ')\\b'
    gLogger.debug('Filter condition:'+filterCondition)
    self.__proxy.setFilterKeyColumn(15)
    self.__proxy.setFilterRegExp(filterCondition)
    for row in xrange(self.__proxy.rowCount()):
      self.tableView.setRowHeight(row, 18)


  #############################################################################
  def showTckFilter(self):
    """shows the tcks"""
    self.tckButton.hide()
    self.tckcloseButton.show()
    self.tckcombo.hide()
    self.filterWidget.show()

  #############################################################################
  def hideTckFilter(self):
    """hides the tcks"""
    self.tckButton.show()
    self.tckcloseButton.hide()
    self.tckcombo.show()
    self.filterWidget.hide()

  #############################################################################
  def getLFNs(self):
    """returns the lfns"""
    lfns = []
    for row in xrange(self.__proxy.rowCount()):
      index = self.__proxy.index(row, 0) # this add the files to my selected list
      lfns += [str(self.__proxy.data(index).toString())]
    return lfns

  #############################################################################
  def waitCursor(self):
    """shows the wait cursor"""
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    """shows the normal cursor"""
    self.setCursor(Qt.ArrowCursor)


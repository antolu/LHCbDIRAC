# pylint: skip-file

"""
Controller of the File Dialog window
"""
########################################################################


__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message

from DIRAC                                                           import S_ERROR
from PyQt4.QtGui                                                     import QMessageBox, QApplication

import sys

#############################################################################
class ControlerFileDialog(ControlerAbstract):
  """
  ControlerFileDialog class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller and the members of the class"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__selectedFiles = []
    self.__dataSet = {}

  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent"""
    if message.action() == 'list':
      self.__list(message)
    elif message.action() == 'listNextFiles':
      self.__list(message)

    return True

  def __list(self, message):
    """list a directory"""
    items = message['items'].getChildren()
    self.getWidget().updateModel(items) # I have to save files.
    self.getWidget().setPath(message['items']['fullpath'])
    self.__selectedFiles = []
    res = self.getWidget().showData(items)


    keys = items.keys()
    if len(keys) > 0:
      value = items[keys[0]]
      self.setDataSet(value['selection'])
      self.getWidget().showSelection(value['selection'])
      if res:

        events = self.countNumberOfEvents(items)
        self.getWidget().showNumberOfEvents(events)

        nbfiles = self.countNumberOfFiles(items)
        self.getWidget().showNumberOfFiles(nbfiles)

        eventinputstat = self.countNumberOfEventInputStat(items)
        self.getWidget().showEventInputStat(eventinputstat)

        filesize = self.getSizeOfFiles(items)
        self.getWidget().showFilesSize(filesize)

        totalLumy = self.countTotalLuminosity(items)
        self.getWidget().showTotalLuminosity(totalLumy)

        luminosity = self.countLuminosity(items)
        self.getWidget().showLuminosity(luminosity)

        self.getWidget().show()

      tcks = self.makeTCKlist(items)
      self.getWidget().fillTckFilter(tcks) # The combobox and the filter listview will be filled
            # using the TCK values from the files
      message = Message({'action':'list', 'items':tcks})
      controlers = self.getChildren()
      controlers['TckFilterWidget'].messageFromParent(message)
    else:
      QMessageBox.information(self.getWidget(),
                              "No data selected!!!",
                              "You have to open Setting/DataQuality menu item and you have to \
                              select data quality flag(s)!",
                              QMessageBox.Ok)

  #############################################################################
  @staticmethod
  def makeTCKlist(data):
    """make the list of tcks"""
    tcks = []
    for i in data:
      if data[i]['TCK'] not in tcks:
        tcks += [data[i]['TCK']]
    return tcks

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the messages send by the children controllers"""
    if message.action() == 'advancedSave':

      sel = message['selection']
      fileName = sel['FileName']
      if fileName.find('.py') < 0:
        fileName += '.py'
      model = self.getWidget().getModel()
      lfns = {}
      if len(self.__selectedFiles) >= 1:
        for i in self.__selectedFiles:
          lfns[i] = model[i]
      else:
        files = self.getWidget().getLFNs()
        for i in files:
          lfns[i] = model[i]

      message = Message({'action':'createCatalog',
                         'fileName':fileName,
                         'lfns':lfns, 'selection':sel,
                         'dataset':self.getDataSet()})
      self.getParent().messageFromChild(self, message)
    elif message.action() == 'applyFilter':
      values = message['items']
      self.getWidget().applyListFilter(values)
    else:
      return self.getParent().messageFromChild(self, message)

  #############################################################################
  def close(self):
    """handles the close button action"""
    #self.getWidget().hide()
    message = Message({'action':'PageSizeIsNull'})
    self.getParent().messageFromChild(self, message)
    self.getWidget().clearTable()

    self.getWidget().clearTable()

    self.getWidget().showSelectedFileSize(0)
    self.getWidget().showSelectedNumberOfEvents(0)
    self.getWidget().showSelectedNumberOfFiles(0)
    self.getWidget().showSelectedTotalLuminosity(0)
    self.getWidget().showSelectedLuminosity(0)
    self.getWidget().setModel({})
    self.getWidget().close()

  #############################################################################
  def save(self):
    """handles the action of the save button"""
    model = self.getWidget().getModel()
    lfns = {}
    if len(self.__selectedFiles) >= 1:
      for i in self.__selectedFiles:
        lfns[i] = model[i]
    else:
      files = self.getWidget().getLFNs()
      for i in files:
        lfns[i] = model[i]

    message = Message({'action':'GetFileName'})
    fileName = self.getParent().messageFromChild(self, message)

    message = Message({'action':'GetPathFileName'})
    fpath = self.getParent().messageFromChild(self, message)

    if fpath != '':
      path = self.getWidget().getPath()
      filedescriptor = open(fpath, 'a')
      filedescriptor.write(path + '\n')
      filedescriptor.close()
      sys.exit(0)
    elif fileName != '':
      message = Message({'action':'SaveToTxt', 'fileName':fileName, 'lfns':lfns})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback:
        QMessageBox.information(self.getWidget(),
                                  "Save As...", "This file has been saved!",
                                  QMessageBox.Ok)
        self.__selectedFiles = []
    else:
      fileName = self.getWidget().getPath()
      fileName = fileName.replace('ALL', '').replace('/', '').replace(' ', '').replace('.', '')
      fileName += '.py'

      fileName, ext = self.getWidget().saveAs(fileName)

      if '.opts' in ext:
        if fileName.find('.opts') < 0:
          fileName += '.opts'
        message = Message({'action':'SaveAs', 'fileName':fileName, 'lfns':lfns, 'dataset':self.getDataSet()})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!", QMessageBox.Ok)
      elif '.txt' in ext:
        if fileName.find('.txt') < 0:
          fileName += '.txt'
        message = Message({'action':'SaveToTxt', 'fileName':fileName, 'lfns':lfns})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!", QMessageBox.Ok)
      elif '.py' in ext:
        if fileName.find('.py') < 0:
          fileName += '.py'
        message = Message({'action':'SaveAs', 'fileName':fileName, 'lfns':lfns, 'dataset':self.getDataSet()})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!", QMessageBox.Ok)

  ############################################################################
  def selection(self, selected, deselected):
    """select the elements of a table"""
    if selected:
      j = -1
      rows = []
      for i in selected.indexes():
        if j != i.row() and i.row() not in rows:
          j = i.row()
          rows += [j]
          if str(i.data().toString()) not in self.__selectedFiles:
            self.__selectedFiles += [str(i.data().toString())]

      self.updateSelectedNbEventType(self.__selectedFiles)
      self.updateSelectedNbFiles(self.__selectedFiles)
      self.updateSelectedFileSize(self.__selectedFiles)
      self.updateselectedNbEventInputStat(self.__selectedFiles)
      self.updateselectedTotalLuminosity(self.__selectedFiles)
      self.updateSelectedLuminosity(self.__selectedFiles)


    if deselected:
      rows = []
      j = -1
      for i in deselected.indexes():
        for i in deselected.indexes():
          if j != i.row() and i.row() not in rows:
            rows += [j]
            j = i.row()
            if str(i.data().toString()) in self.__selectedFiles:
              self.__selectedFiles.remove(str(i.data().toString()))
              self.updateSelectedNbEventType(self.__selectedFiles)
              self.updateSelectedNbFiles(self.__selectedFiles)
              self.updateSelectedFileSize(self.__selectedFiles)
              self.updateselectedNbEventInputStat(self.__selectedFiles)
              self.updateselectedTotalLuminosity(self.__selectedFiles)
              self.updateSelectedLuminosity(self.__selectedFiles)

  #############################################################################
  @staticmethod
  def countNumberOfEvents(items):
    """counts the number of events"""
    eventnum = 0
    for item in items:
      value = items[item]
      if value['EventStat'] != None:
        eventnum += int(value['EventStat'])
    return eventnum

  #############################################################################
  @staticmethod
  def countNumberOfEventInputStat(items):
    """counts the number of imput events"""
    eventinputstat = 0
    for item in items:
      value = items[item]
      if value['EventInputStat'] != None:
        eventinputstat += int(value['EventInputStat'])
    return eventinputstat

  #############################################################################
  @staticmethod
  def countNumberOfFiles(items):
    """returns the number of files"""
    return len(items)

  #############################################################################
  @staticmethod
  def countTotalLuminosity(items):
    """calculates the total luminosity"""
    luminosity = 0
    for item in items:
      value = items[item]
      if value['TotalLuminosity'] != None:
        luminosity += float(value['TotalLuminosity'])
    return luminosity

  #############################################################################
  @staticmethod
  def countLuminosity(items):
    """calculates the luminosity"""
    luminosity = 0
    for item in items:
      value = items[item]
      if value['Luminosity'] != None:
        luminosity += float(value['Luminosity'])
    return luminosity

  #############################################################################
  def updateSelectedNbEventType(self, files):
    """updates the selected number of events"""
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    nbevents = self.countNumberOfEvents(lfns)
    self.getWidget().showSelectedNumberOfEvents(nbevents)

  #############################################################################
  def updateselectedNbEventInputStat(self, files):
    """updates the selected input events"""
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    eventinputstat = self.countNumberOfEventInputStat(lfns)
    self.getWidget().showSelectedEventInputStat(eventinputstat)

  #############################################################################
  def updateselectedTotalLuminosity(self, files):
    """updates the selected total luminosity"""
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    totalLuminosity = self.countTotalLuminosity(lfns)
    self.getWidget().showSelectedTotalLuminosity(totalLuminosity)

  #############################################################################
  def updateSelectedLuminosity(self, files):
    """updates the luminosity"""
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    totalLuminosity = self.countLuminosity(lfns)
    self.getWidget().showSelectedLuminosity(totalLuminosity)

  #############################################################################
  def updateSelectedNbFiles(self, files):
    """updates the number of files"""
    self.getWidget().showSelectedNumberOfFiles(len(files))

  #############################################################################
  @staticmethod
  def getSizeOfFiles(items):
    """returns the size of the files"""
    size = 0
    for item in items:
      value = items[item]
      if str(value['FileSize']) != 'None':
        size += int(value['FileSize'])
    return size / 1000000000.

  #############################################################################
  def updateSelectedFileSize(self, files):
    """updates the size of the selected files"""
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    filesize = self.getSizeOfFiles(lfns)
    self.getWidget().showSelectedFileSize(filesize)

  #############################################################################
  def jobinfo(self):
    """handles the job info action"""
    if len(self.__selectedFiles) != 0:
      message = Message({'action':'JobInfo', 'fileName':self.__selectedFiles[0]})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback.action() == 'showJobInfos':
        controlers = self.getChildren()
        ct = controlers['HistoryDialog']
        feedback = ct.messageFromParent(feedback)
      return S_ERROR(feedback)


  #############################################################################
  def getancesstots(self):
    """handles the ancesstors action"""
    message = Message({'action':'getAnccestors', 'files':self.__selectedFiles[0]})
    feedback = self.getParent().messageFromChild(self, message)
    action = feedback.action()
    if action == 'error':
      self.getWidget().showError(feedback['message'])
    elif action == 'showAncestors':
      controlers = self.getChildren()
      ct = controlers['HistoryDialog']
      message = feedback['files']
      message = Message({'action':'list', 'items':message})
      feedback = ct.messageFromParent(message)
    else:
      self.getWidget().showError('Unkown message' + str(message))

  #############################################################################
  def loggininginfo(self):
    """handles the login info action"""
    message = Message({'action':'logfile', 'fileName':self.__selectedFiles})
    feedback = self.getParent().messageFromChild(self, message)
    action = feedback.action()
    if action == 'error':
      self.getWidget().showError(feedback['message'])
    elif action == 'showLog':
      controlers = self.getChildren()
      controlers['LogFileWidget'].messageFromParent(feedback)

  #############################################################################
  def advancedSave(self):
    """handles the advanced save action"""
    message = Message({'action':'showWidget'})
    controlers = self.getChildren()
    controlers['AdvancedSave'].messageFromParent(message)

  #############################################################################
  def next(self):
    """handles the action of the next button"""
    path = self.getWidget().getPath()
    message = Message({'action':'getLimitedFiles', 'path':path})
    self.getParent().messageFromChild(self, message)

  #############################################################################
  def tckChanged(self, i):
    """handles the tck action"""
    self.getWidget().applyFilter(str(i))

  #############################################################################
  def tckButtonPressed(self):
    """handles the action of the tck button"""
    self.getWidget().showTckFilter()

  #############################################################################
  def hideFilterWidget(self):
    """hides the widget"""
    self.getWidget().hideTckFilter()

  #############################################################################
  def copy(self):
    """copy the selected data"""
    clipboard = QApplication.clipboard()
    self.getWidget().getModel()
    text = ''
    if len(self.__selectedFiles) >= 1:
      for i in self.__selectedFiles:
        text += i + "\n"
    else:
      files = self.getWidget().getLFNs()
      for i in files:
        text += i + "\n"
    clipboard.setText(text)

  #############################################################################
  def setDataSet(self, in_dict):
    """sets the dataset"""
    self.__dataSet = in_dict

  #############################################################################
  def getDataSet(self):
    """returns the dataset"""
    return self.__dataSet



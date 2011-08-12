########################################################################
# $HeadURL$
########################################################################


__RCSID__ = "$Id$"

from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.Message                       import Message
from LHCbDIRAC.NewBookkeepingSystem.Gui.ProgressBar.ProgressThread          import ProgressThread
from LHCbDIRAC.NewBookkeepingSystem.Gui.Widget.LogFileWidget                import LogFileWidget

from DIRAC                                                           import gLogger, S_OK, S_ERROR

from PyQt4.QtGui                                                     import *

import sys
#############################################################################
class ControlerFileDialog(ControlerAbstract):

  #############################################################################
  def __init__(self, widget, parent):
    super(ControlerFileDialog, self).__init__(widget, parent)
    self.__selectedFiles = []

  #############################################################################
  def messageFromParent(self, message):
    if message.action()=='list':
      self.__list(message)
    elif message.action()=='listNextFiles':
      self.__list(message, update=True)

    return True

  def __list(self, message, update=False):
    items = message['items'].getChildren()
    self.getWidget().updateModel(items) # I have to save files.
    self.getWidget().setPath(message['items']['fullpath'])
    self.__selectedFiles = []
    res = self.getWidget().showData(items)


    keys = items.keys()
    if len(keys) > 0:
      value = items[keys[0]]
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

        Luminosity = self.countLuminosity(items)
        self.getWidget().showLuminosity(Luminosity)

        self.getWidget().show()

      tcks = self.makeTCKlist(items)
      self.getWidget().fillTckFilter(tcks) # The combobox and the filter listview will be filled using the TCK values from the files
      message = Message({'action':'list','items':tcks})
      controlers = self.getChildren()
      controlers['TckFilterWidget'].messageFromParent(message)

    else:
      QMessageBox.information(self.getWidget(), "No data selected!!!", "You have to open Setting/DataQuality menu item and you have to select data quality flag(s)!",QMessageBox.Ok)

  #############################################################################
  def makeTCKlist(self, data):
    tcks = []
    for i in data:
      if data[i]['TCK'] not in tcks:
        tcks += [data[i]['TCK']]
    return tcks

  #############################################################################
  def messageFromChild(self, sender, message):
    if message.action()=='advancedSave':

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
        for file in model:
          lfns[file] = model[file]

      message = Message({'action':'createCatalog','fileName':fileName,'lfns':lfns,'selection':sel})
      feedback = self.getParent().messageFromChild(self, message)
    elif message.action() == 'applyFilter':
      values = message['items']
      self.getWidget().applyListFilter(values)
    else:
      return self.getParent().messageFromChild(self, message)

  #############################################################################
  def close(self):
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
    model = self.getWidget().getModel()
    lfns = {}
    if len(self.__selectedFiles) >= 1:
      for i in self.__selectedFiles:
        lfns[i] = model[i]
    else:
      for file in model:
        lfns[file] = model[file]

    message = Message({'action':'GetFileName'})
    fileName = self.getParent().messageFromChild(self, message)

    message = Message({'action':'GetPathFileName'})
    fpath = self.getParent().messageFromChild(self, message)

    if fpath !='':
      path = self.getWidget().getPath()
      f=open(fpath,'a')
      f.write(path+'\n')
      f.close()
      sys.exit(0)
    elif fileName != '':
      message = Message({'action':'SaveToTxt','fileName':fileName,'lfns':lfns})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
          self.__selectedFiles = []
    else:
      fileName = self.getWidget().getPath()
      fileName = fileName.replace('/CFGN_','').replace('/CFGV','').replace('/SCON','').replace('/PAS','').replace('/EVT','').replace('/PROD','').replace('FTY','').replace('ALL','').replace('/','').replace(' ','').replace('.','')
      fileName += '.py'

      fileName,ext = self.getWidget().saveAs(fileName)

      if '.opts' in ext:
        if fileName.find('.opts') < 0:
          fileName += '.opts'
        message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
      elif '.txt' in ext:
        if fileName.find('.txt') < 0:
          fileName += '.txt'
        message = Message({'action':'SaveToTxt','fileName':fileName,'lfns':lfns})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
      elif '.py' in ext:
        if fileName.find('.py') < 0:
          fileName += '.py'
        message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback:
          QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)

  ############################################################################
  def selection(self, selected, deselected):
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
  def countNumberOfEvents(self, items):
    eventnum = 0;
    for item in items:
      value = items[item]
      if value['EventStat'] != None:
        eventnum += int(value['EventStat'])
    return eventnum

  #############################################################################
  def countNumberOfEventInputStat(self, items):
    eventinputstat = 0;
    for item in items:
      value = items[item]
      if value['EventInputStat'] != None:
        eventinputstat += int(value['EventInputStat'])
    return eventinputstat

  #############################################################################
  def countNumberOfFiles(self, items):
    nbfiles = 0;
    for f in items:
      nbfiles += 1
    return nbfiles

  #############################################################################
  def countTotalLuminosity(self, items):
    luminosity = 0;
    for item in items:
      value = items[item]
      if value['TotalLuminosity'] != None:
        luminosity += float(value['TotalLuminosity'])
    return luminosity

  #############################################################################
  def countLuminosity(self, items):
    luminosity = 0;
    for item in items:
      value = items[item]
      if value['Luminosity'] != None:
        luminosity += float(value['Luminosity'])
    return luminosity

  #############################################################################
  def updateSelectedNbEventType(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    nbevents = self.countNumberOfEvents(lfns)
    self.getWidget().showSelectedNumberOfEvents(nbevents)

  #############################################################################
  def updateselectedNbEventInputStat(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    eventinputstat = self.countNumberOfEventInputStat(lfns)
    self.getWidget().showSelectedEventInputStat(eventinputstat)

  #############################################################################
  def updateselectedTotalLuminosity(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    totalLuminosity = self.countTotalLuminosity(lfns)
    self.getWidget().showSelectedTotalLuminosity(totalLuminosity)

  #############################################################################
  def updateSelectedLuminosity(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    totalLuminosity = self.countLuminosity(lfns)
    self.getWidget().showSelectedLuminosity(totalLuminosity)

  #############################################################################
  def updateSelectedNbFiles(self, files):
    self.getWidget().showSelectedNumberOfFiles(len(files))

  #############################################################################
  def getSizeOfFiles(self, items):
    size = 0;
    for item in items:
      value = items[item]
      if str(value['FileSize']) != 'None':
        size += int(value['FileSize'])
    return size/1000000000.

  #############################################################################
  def updateSelectedFileSize(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    filesize = self.getSizeOfFiles(lfns)
    self.getWidget().showSelectedFileSize(filesize)

  #############################################################################
  def jobinfo(self):
    if len(self.__selectedFiles) != 0:
      message = Message({'action':'JobInfo','fileName':self.__selectedFiles[0]})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback.action() == 'showJobInfos':
        controlers = self.getChildren()
        ct = controlers['HistoryDialog']
        feedback = ct.messageFromParent(feedback)
      return S_ERROR(feedback)


  #############################################################################
  def getancesstots(self):
    message = Message({'action':'getAnccestors','files':self.__selectedFiles[0]})
    feedback = self.getParent().messageFromChild(self, message)
    action = feedback.action()
    if action == 'error':
      self.getWidget().showError(feedback['message'])
    elif action == 'showAncestors':
      controlers = self.getChildren()
      ct = controlers['HistoryDialog']
      message = feedback['files']
      message = Message({'action':'list','items':message})
      feedback = ct.messageFromParent(message)
    else:
      self.getWidget().showError('Unkown message'+str(message))

  #############################################################################
  def loggininginfo(self):
    message = Message({'action':'logfile','fileName':self.__selectedFiles})
    feedback = self.getParent().messageFromChild(self, message)
    action = feedback.action()
    if action == 'error':
      self.getWidget().showError(feedback['message'])
    elif action == 'showLog':
      controlers = self.getChildren()
      controlers['LogFileWidget'].messageFromParent(feedback)

  #############################################################################
  def advancedSave(self):
    message = Message({'action':'showWidget'})
    controlers = self.getChildren()
    controlers['AdvancedSave'].messageFromParent(message)

  #############################################################################
  def next(self):
    path = self.getWidget().getPath()
    message = Message({'action':'getLimitedFiles','path':path})
    feedback = self.getParent().messageFromChild(self, message)

  #############################################################################
  def tckChanged(self, i):
    self.getWidget().applyFilter(str(i))

  #############################################################################
  def tckButtonPressed(self):
    self.getWidget().showTckFilter()

  #############################################################################
  def hideFilterWidget(self):
    self.getWidget().hideTckFilter()

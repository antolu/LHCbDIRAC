########################################################################
# $Id: ControlerFileDialog.py,v 1.4 2008/10/18 18:36:52 zmathe Exp $
########################################################################


__RCSID__ = "$Id: ControlerFileDialog.py,v 1.4 2008/10/18 18:36:52 zmathe Exp $"

from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from PyQt4.QtGui                                                     import *
from DIRAC.BookkeepingSystem.Gui.ProgressBar.ProgressThread          import ProgressThread

#############################################################################  
class ControlerFileDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerFileDialog, self).__init__(widget, parent)
    self.__selectedFiles = []
    #self.__progressBar = ProgressThread(False, 'Query on database...',self.getWidget())
  
  #############################################################################  
  def messageFromParent(self, message):
    if message.action()=='list':
      '''
      if self.__progressBar.isRunning():
          gLogger.info('2')
          self.__progressBar.stop()
          self.__progressBar.wait()
      self.__progressBar.start()  
      '''
      items = message['items'].getChildren()
      self.getWidget().setModel(items) # I have to save files.
      self.__selectedFiles = []
      res = self.getWidget().showData(items)
      
      keys = items.keys()
      value = items[keys[0]]
      self.getWidget().showSelection(value['Selection'])
      if res:
        
        events = self.countNumberOfEvents(items)
        self.getWidget().showNumberOfEvents(events)
        
        nbfiles = self.countNumberOfFiles(items)
        self.getWidget().showNumberOfFiles(nbfiles)
        
        filesize = self.getSizeOfFiles(items)
        self.getWidget().showFilesSize(filesize)
        
        #self.__progressBar.stop()
        #self.__progressBar.wait()
        
        self.getWidget().show()
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def close(self):
    #self.getWidget().hide()
    self.getWidget().showSelectedFileSize(0)
    self.getWidget().showSelectedNumberOfEvents(0)
    self.getWidget().showSelectedNumberOfFiles(0)
    self.getWidget().close()
    
  #############################################################################  
  def save(self):
    if len(self.__selectedFiles) > 1:
      fileName,ext = self.getWidget().saveAs()
      print ext
      if fileName <>'':
        model = self.getWidget().getModel()
        lfns = {}
        for i in self.__selectedFiles:
          lfns[i] = model[i]
        if '.opts' in ext:   
          message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback:
            QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
            self.__selectedFiles = []
        elif '.txt' in ext:
          f = open(fileName,'w')
          for file in self.__selectedFiles:
            f.write(file+'\n')       
          self.__selectedFiles = [] 
        elif '.py' in ext:
          message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback:
            QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
            self.__selectedFiles = []
          
    else:
      fileName,ext = self.getWidget().saveAs()
      
      if fileName <> '':
        model = self.getWidget().getModel()
        lfns = {}
        for file in model:
          lfns[file] = model[file]  
        if '.xml' in ext:
          print 'xml'
        elif '.opts' in ext:
          message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback:
            QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
            self.__selectedFiles = []
        elif '.py' in ext:
          message = Message({'action':'SaveAs','fileName':fileName,'lfns':lfns})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback:
            QMessageBox.information(self.getWidget(), "Save As...", "This file has been saved!",QMessageBox.Ok)
            self.__selectedFiles = []
        
        elif '.txt' in ext:
          f = open(fileName,'w')
          for file in model:
            f.write(file+'\n')
          self.__selectedFiles = []
          
        
    
  #############################################################################  
  def selection(self, selected, deselected):
    if selected:
      row = selected.indexes()[0].row()
      data = selected.indexes()[0].model().arraydata[row][0]
      self.__selectedFiles += [data]
      self.updateSelectedNbEventType(self.__selectedFiles)
      self.updateSelectedNbFiles(self.__selectedFiles)
      self.updateSelectedFileSize(self.__selectedFiles)
      #print 'Selection',selected.indexes()[0].model().arraydata[row][0]
    
    if deselected:
      row = deselected.indexes()[0].row()
      for i in deselected.indexes():
        row = i.row()
        data = i.model().arraydata[row][0]
        if data in self.__selectedFiles:
          self.__selectedFiles.remove(data)
          self.updateSelectedNbEventType(self.__selectedFiles)
          self.updateSelectedNbFiles(self.__selectedFiles)
          self.updateSelectedFileSize(self.__selectedFiles)
      
  #############################################################################  
  def countNumberOfEvents(self, items):
    eventnum = 0;
    for item in items:
      value = items[item]
      eventnum += int(value['EventStat'])
    return eventnum
  
  #############################################################################  
  def countNumberOfFiles(self, items):
    nbfiles = 0;
    for f in items:
      nbfiles += 1
    return nbfiles
  
  #############################################################################  
  def updateSelectedNbEventType(self, files):
    model = self.getWidget().getModel()
    lfns = {}
    for i in files:
      lfns[i] = model[i]
    nbevents = self.countNumberOfEvents(lfns)
    self.getWidget().showSelectedNumberOfEvents(nbevents)
  
  #############################################################################  
  def updateSelectedNbFiles(self, files):
    self.getWidget().showSelectedNumberOfFiles(len(files))
         
  #############################################################################  
  def getSizeOfFiles(self, items):
    size = 0;
    for item in items:
      value = items[item]
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
    message = Message({'action':'JobInfo','fileName':self.__selectedFiles[0]})
    feedback = self.getParent().messageFromChild(self, message)
    
  
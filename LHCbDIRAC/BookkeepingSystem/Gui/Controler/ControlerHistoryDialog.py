########################################################################
# $Id$
########################################################################


__RCSID__ = "$Id$"

from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from DIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.BookkeepingSystem.Gui.Widget.HistoryNavigationCommand     import HistoryNavigationCommand
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from PyQt4.QtGui                                                     import *
import sys
#############################################################################  
class ControlerHistoryDialog(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerHistoryDialog, self).__init__(widget, parent)
    self.__selectedFiles = []
    self.__comands = []
    self.__current = 0
      
  #############################################################################  
  def messageFromParent(self, message):
    if message.action() =='list':
      values = message['items']
      headers= values['ParameterNames']
      data= values['Records']
      if len(data) == 0:
        self.getWidget().showError('This is the last file!')
      else:
        if len(self.__comands) == self.__current:
          tm = self.getWidget().filltable(headers, data, self.getWidget().getFilesTableView())
          hcommand = HistoryNavigationCommand(self.getWidget(), self.getWidget().getFilesTableView(), tm)
          self.__comands += [hcommand]
          self.__current += 1
          hcommand.execute()
        
    elif message.action() == 'showJobInfos':
      values = message['items']
      headers = ['Name','Value']
      data = []
      value = []
      for i in values.keys():
        d = values[i]
        if d == None:
          d = ''
        data += [ [i,d] ]
        
      self.getWidget().filltable(headers, data, self.getWidget().getJobTableView())
      self.getWidget().show()
      
    else:
      gLogger.error('Unkown message')
      return S_ERROR('Unkown message')
    
  
  #############################################################################  
  def messageFromChild(self, sender, message):
      return self.getParent().messageFromChild(self, message)
  
  #############################################################################  
  def selection(self, selected, deselected):
    if selected:
      for i in selected.indexes():
        row = i.row()
        data = i.model().arraydata[row][1]
        if data not in self.__selectedFiles:
          self.__selectedFiles = [data]
      message = Message({'action':'JobInfo','fileName':self.__selectedFiles[0]})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback.action() == 'showJobInfos':
        values = feedback['items']
        headers = ['Name','Value']
        data = []
        value = []
        for i in values.keys():
          d = values[i]
          if d == None:
            d = ''
          data += [ [i,d] ]
          
        self.getWidget().filltable(headers, data, self.getWidget().getJobTableView())
        self.getWidget().setNextButtonState(enable = True)
      
    if deselected:
      row = deselected.indexes()[0].row()
      for i in deselected.indexes():
        row = i.row()
        data = i.model().arraydata[row][0]
        if data in self.__selectedFiles:
          self.__selectedFiles.remove(data)
          
  #############################################################################  
  def close(self):
    self.__current = 0
    self.__comands = []
    self.getWidget().close()
  
  #############################################################################
  def next(self):
    self.getWidget().setBackButtonSatate(enable = True)
    if len(self.__comands)  == self.__current:
      self.getWidget().setNextButtonState(enable = False)
    if len(self.__selectedFiles) > 0:
      self.__current += 1
      if len(self.__comands) < self.__current:
        message = Message({'action':'getAnccestors','files':self.__selectedFiles[0]})
        feedback = self.getParent().messageFromChild(self, message)
        values = feedback['files']
        headers= values['ParameterNames']
        data= values['Records']
        if len(data) == 0:
          self.getWidget().showError('This is the last file!')
        else:          
          tm = self.getWidget().filltable(headers, data, self.getWidget().getFilesTableView())
          hcommand = HistoryNavigationCommand(self.getWidget(), self.getWidget().getFilesTableView(), tm)
          self.__comands += [hcommand]
          hcommand.execute()
      else:
        hcommand = self.__comands[self.__current - 1]
        hcommand.execute()          
    else:
      self.getWidget().showError('Please select a file!')
    
  
  #############################################################################
  def back(self):
    self.__current -= 1
    hcommand = self.__comands[self.__current - 1 ]
    hcommand.execute()
    self.getWidget().setNextButtonState(enable = True)
    if self.__current == 1: 
      self.getWidget().setBackButtonSatate(enable = False)
      
    

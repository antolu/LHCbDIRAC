########################################################################
# $HeadURL:  $
########################################################################


__RCSID__ = "$Id: $"

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.Core.DISET.RPCClient                                          import RPCClient
from DIRAC.FrameworkSystem.Client.UserProfileClient                      import UserProfileClient
from PyQt4.QtCore import *
from PyQt4.QtGui import *

from DIRAC                                                           import gLogger, S_OK, S_ERROR

import sys

#############################################################################  
class ControlerBookmarks(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerBookmarks, self).__init__(widget, parent)
    self.__selectedFiles = []
    '''  
sim+adv//
sim+std//
evt+adv
evt+std
prd
run
'''        

  #############################################################################  
  def messageFromParent(self, message):
    if message.action() == 'showValues':
      controlers = self.getChildren()
      ct = controlers['AddBookmarks']   
      return ct.messageFromParent(message)
    else:
      gLogger.error('Unkown message', message)
      return S_ERROR('Unkown message')
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    if message.action()=='addBookmarks':
      bookmarks = message['bookmark']
      retVal  = self.__addBookmark(bookmarks['Path'], bookmarks['Title'])
      if retVal['OK']:
        self.filltable()
      else:
        return S_ERROR(retVal['Message'])
      return S_OK()
    else:
      gLogger.error('Unkown message:ControlerBookmarks', message)
    return S_ERROR('Unkown message')
  
  def filltable(self):
    header = ['Title','Path']
    tabledata = []
    retVal = self.__getBookmarks()
    if retVal['OK']:
      bookmarks = retVal['Value']
      for i in bookmarks:
        tabledata += [[i, bookmarks[i]]]
      if len(tabledata) > 0:
        self.getWidget().filltable(header, tabledata)
      else:
        tabledata = [['','']]
        self.getWidget().filltable(header, tabledata)
    else:
      gLogger.info('You do not have bookmarks!',retVal['Message'])
        
    
  #############################################################################
  def __getBookmarks(self):
    upc = UserProfileClient( "Bookkeeping", RPCClient )
    return upc.retrieveVar( "Bookmarks" )

  #############################################################################
  def __addBookmark(self,path,title):
    upc = UserProfileClient( "Bookkeeping", RPCClient )
    result = upc.retrieveVar( "Bookmarks" )
    if result["OK"]:
      data = result["Value"]
    else:
      data = {}
    if title in data:
      QMessageBox.critical(self.getWidget(), "Error","The bookmark with the title \"" + title + "\" is already exists",QMessageBox.Ok)
      return S_ERROR("The bookmark with the title \"" + title + "\" is already exists")
    else:
      data[title] = path
    result = upc.storeVar( "Bookmarks", data )
    if result["OK"]:
      return self.__getBookmarks()
    else:
      return S_ERROR(result["Message"])
  
  #############################################################################
  def __delBookmark(self,path,title):
    upc = UserProfileClient( "Bookkeeping", RPCClient )
    result = upc.retrieveVar( "Bookmarks" )
    if result["OK"]:
      data = result["Value"]
    else:
      data = {}
    if title in data:
      del data[title]
    else:
      QMessageBox.critical(self.getWidget(), "Error","Can't delete not existing bookmark: \"" + title + "\"",QMessageBox.Ok)
      return S_ERROR("Can't delete not existing bookmark: \"" + title + "\"")
    result = upc.storeVar( "Bookmarks", data )
    if result["OK"]:
      return self.__getBookmarks()
    else:
      return S_ERROR(result["Message"])

    
  
  #############################################################################
  def removeBookmarks(self):
    row = self.getWidget().getSelectedRow()
    retVal = self.__delBookmark(row['Path'], row['Title'])
    if not retVal['OK']:
      gLogger.error(retVal['Message'])
    else:
      self.filltable()
      
  #############################################################################
  def addBookmarks(self):
    controlers = self.getChildren()
    ct = controlers['AddBookmarks']   
    message = Message({'action':'showWidget'})
    ct.messageFromParent(message)
    
  #############################################################################  
  def selection(self, selected, deselected):
    if selected:
      for i in selected.indexes():
        row = i.row()
        data = i.model().arraydata[row][1]
        if data not in self.__selectedFiles:
          self.__selectedFiles = [data]
          message = Message({'action':'openPathLocation','Path':data})
          self.getParent().messageFromChild(self, message)
    if deselected:
      row = deselected.indexes()[0].row()
      for i in deselected.indexes():
        row = i.row()
        data = i.model().arraydata[row][0]
        if data in self.__selectedFiles:
          self.__selectedFiles.remove(data)
  
  def doubleclick(self, item):
    gLogger('Not implemented!')   
      
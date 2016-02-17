# pylint: skip-file

"""
Controlls the Bookmarks widget
"""
########################################################################


__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC.Core.DISET.RPCClient                                          import RPCClient
from DIRAC.FrameworkSystem.Client.UserProfileClient                      import UserProfileClient
from PyQt4.QtGui                                                         import QMessageBox

from DIRAC                                                               import gLogger, S_OK, S_ERROR

#############################################################################
class ControlerBookmarks(ControlerAbstract):
  """
  ControlerBookmarks class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__selectedFiles = []

#sim+adv//
#sim+std//
#evt+adv
#evt+std
#prd
#run


  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by parent"""
    if message.action() == 'showValues':
      controlers = self.getChildren()
      ct = controlers['AddBookmarks']
      return ct.messageFromParent(message)
    else:
      gLogger.error('Unkown message', message)
      return S_ERROR('Unkown message')

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the messages sent from its children"""
    if message.action() == 'addBookmarks':
      bookmarks = message['bookmark']
      retVal = self.__addBookmark(bookmarks['Path'], bookmarks['Title'])
      if retVal['OK']:
        self.filltable()
      else:
        return S_ERROR(retVal['Message'])
      return S_OK()
    else:
      gLogger.error('Unkown message:ControlerBookmarks', message)
    return S_ERROR('Unkown message')

  def filltable(self):
    """used to fill a table widget"""
    header = ['Title', 'Path']
    tabledata = []
    retVal = self.__getBookmarks()
    if retVal['OK']:
      bookmarks = retVal['Value']
      for i in bookmarks:
        tabledata += [[i, bookmarks[i]]]
      if len(tabledata) > 0:
        self.getWidget().filltable(header, tabledata)
      else:
        tabledata = [['', '']]
        self.getWidget().filltable(header, tabledata)
    else:
      gLogger.info('You do not have yet bookmarks for the bookkeeping !', retVal['Message'])

  #############################################################################
  def __getBookmarks(self):
    """returns the bookkmarks"""
    self.getWidget().waitCursor()
    upc = UserProfileClient("Bookkeeping", RPCClient)
    bookmarks = upc.retrieveVar("Bookmarks")
    self.getWidget().arrowCursor()
    return bookmarks
    #return S_ERROR('FIGYELEM!!!')

  #############################################################################
  def __addBookmark(self, path, title):
    """adds a bookmark"""
    self.getWidget().waitCursor()
    upc = UserProfileClient("Bookkeeping", RPCClient)
    result = upc.retrieveVar("Bookmarks")
    if result["OK"]:
      data = result["Value"]
    else:
      data = {}
    if title in data:
      QMessageBox.critical(self.getWidget(),
                           "Error", "The bookmark with the title \"" + title + "\" is already exists",
                           QMessageBox.Ok)
      return S_ERROR("The bookmark with the title \"" + title + "\" is already exists")
    else:
      data[title] = path
    result = upc.storeVar("Bookmarks", data)
    self.getWidget().arrowCursor()
    if result["OK"]:
      return self.__getBookmarks()
    else:
      return S_ERROR(result["Message"])

  #############################################################################
  def __delBookmark(self, title):
    """deletes a bookmark"""
    self.getWidget().waitCursor()
    upc = UserProfileClient("Bookkeeping", RPCClient)
    result = upc.retrieveVar("Bookmarks")
    if result["OK"]:
      data = result["Value"]
    else:
      data = {}
    if title in data:
      del data[title]
    else:
      QMessageBox.critical(self.getWidget(),
                           "Error", "Can't delete not existing bookmark: \"" + title + "\"",
                           QMessageBox.Ok)
      return S_ERROR("Can't delete not existing bookmark: \"" + title + "\"")
    result = upc.storeVar("Bookmarks", data)
    self.getWidget().arrowCursor()
    if result["OK"]:
      return self.__getBookmarks()
    else:
      return S_ERROR(result["Message"])

  #############################################################################
  def removeBookmarks(self):
    """handles the remove bookmarks"""
    row = self.getWidget().getSelectedRow()
    retVal = self.__delBookmark(row['Title'])
    if not retVal['OK']:
      gLogger.error(retVal['Message'])
    else:
      self.filltable()

  #############################################################################
  def addBookmarks(self):
    """handles the addBookmarks action"""
    controlers = self.getChildren()
    ct = controlers['AddBookmarks']
    message = Message({'action':'showWidget'})
    ct.messageFromParent(message)

  #############################################################################
  def selection(self, selected, deselected):
    """handle the selections"""
    if selected:
      for i in selected.indexes():
        row = i.row()
        data = i.model().arraydata[row][1]
        if data not in self.__selectedFiles:
          self.__selectedFiles = [data]
          message = Message({'action':'openPathLocation', 'Path':data})
          self.getParent().messageFromChild(self, message)
    if deselected:
      row = deselected.indexes()[0].row()
      for i in deselected.indexes():
        row = i.row()
        data = i.model().arraydata[row][0]
        if data in self.__selectedFiles:
          self.__selectedFiles.remove(data)

  def doubleclick(self, item):
    """handles the double clicks"""
    pass


"""
Controls the Bookkeeping trees.
"""
########################################################################
# $Id$
########################################################################


from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract         import ControlerAbstract
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.Message                       import Message
from DIRAC                                                               import gLogger

from PyQt4.QtGui   import QMessageBox
from PyQt4.QtCore  import Qt
import types
__RCSID__ = "$Id$"

#############################################################################
class ControlerTree(ControlerAbstract):
  """
  ControlerTree class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)
    self.__pagesize = 0
    self.__option = None
    self.__offset = 0
  #############################################################################
  def messageFromParent(self, message):
    """handles the messages sent by the parent controller"""
    gLogger.debug(message)
    if message.action() == 'list':
      self.getWidget().getTree().showTree(message['items'])
    elif message.action() == 'removeTree':
      self.getWidget().getTree().clearTree()
      self.getWidget().getTree().showTree(message['items'])
    elif message.action() == 'showJobInfos':
      controlers = self.getChildren()
      controlers['InfoDialog'].messageFromParent(message)
    else:
      print 'Unknown message!', message.action()

  #############################################################################
  def messageFromChild(self, sender, message):
    """handles the messages sent by the children controllers"""
    if message.action() == 'getLimitedFiles':
      message = Message({'action':'expande',
                         'node':message['path'],
                         'StartItem':self.__pagesize,
                         'MaxItem':(self.__pagesize + self.__offset)})

      self.__pagesize += self.__offset
      feedback = self.getParent().messageFromChild(self, message)

      if feedback.action() == 'showNode':
        controlers = self.getChildren()
        ct = controlers['FileDialog']
        message = Message({'action':'listNextFiles', 'items':feedback['items']})
        ct.messageFromParent(message)
    elif message.action() == 'PageSizeIsNull':
      self.__offset = 0
      self.__pagesize = 0
    elif message.action() == 'openPathLocation':
      path = message['Path']
      self.browsePath(path)
    else:
      return self.getParent().messageFromChild(self, message) # send to main controler!

  #############################################################################
  def configButton(self):
    """handles the action of the configuration button"""
    message = Message({'action':'configbuttonChanged'})
    self.getParent().messageFromChild(self, message)

  def productionRadioButton(self):
    """handles the action of the production radio button"""
    message = Message({'action':'productionButtonChanged'})
    self.getParent().messageFromChild(self, message)

  #############################################################################
  def runRadioButton(self):
    """handles the action of the run button"""
    message = Message({'action':'runLookup'})
    self.getParent().messageFromChild(self, message)

  #############################################################################
  def bookmarksButtonPressed(self):
    """handles the action of the bookmarks button"""
    self.getWidget().showBookmarks()

  def hidewidget(self):
    """hides the bookmarks widget"""
    self.getWidget().hidewidget()

  #############################################################################
  def standardQuery(self):
    """handles the action of the standard query check box"""
    widget = self.getWidget()

    if not widget.runLookupRadioButtonIsChecked() and not widget.productionLookupradiobuttonIschecked():
      message = Message({'action':'StandardQuery'})
      self.getParent().messageFromChild(self, message)
      self.getWidget().setAdvancedQueryValue()
    else:
      self.getWidget().setAdvancedQueryValue()

  #############################################################################
  def advancedQuery(self):
    """handles the action of the advanced query check box"""
    widget = self.getWidget()

    if not widget.runLookupRadioButtonIsChecked() and not widget.productionLookupradiobuttonIschecked():
      message = Message({'action':'AdvancedQuery'})
      self.getParent().messageFromChild(self, message)
      self.getWidget().setStandardQueryValue()
    else:
      self.getWidget().setStandardQueryValue()

  #############################################################################
  def eventTypeButton(self):
    """handles the action of the event type button"""
    message = Message({'action':'eventbuttonChanged'})
    self.getParent().messageFromChild(self, message)

  #############################################################################
  def on_item_expanded(self, parentItem):
    """expand on node of the tree"""
    gLogger.debug('On item expanded', parentItem.getUserObject())
    node = parentItem.getUserObject()
    if node != None:
      path = node['fullpath']
      if parentItem.childCount() != 1:
        return
        #parentItem.takeChild(0) # I have to remove the dumychildren!
      else:
        if parentItem.childCount() == 1:
          child = parentItem.child(0)
          gLogger.debug('Childcount:', child.getUserObject())
          parentItem.takeChild(0)

        if node.has_key('showFiles'):
          message = Message({'action':'waitCursor', 'type':None})
          self.getParent().messageFromChild(self, message)

          message = Message({'action':'getNbEventsAndFiles', 'node':path})
          feedback = self.getParent().messageFromChild(self, message)

          statistics = feedback['Extras']['GlobalStatistics']
          files = feedback['TotalRecords']
          nbev = statistics['Number of Events']
          show = {'Number of files':files, 'Nuber of Events':nbev}
          #show={'Number of files':feedback['TotalRecords'],'Nuber of Events':feedback['Extras']['Number of Events']}
          self.getWidget().getTree().addLeaf(show, parentItem)
          message = Message({'action':'arrowCursor', 'type':None})
          self.getParent().messageFromChild(self, message)
        else:
          message = Message({'action':'expande','node':path})
          feedback = self.getParent().messageFromChild(self, message)
          if feedback.action() == 'showNode':
            if feedback['items'].childnum() > 0:
              child = feedback['items'].child(0)
              if not child['expandable']:
                message = Message({'action':'waitCursor', 'type':None})
                self.getParent().messageFromChild(self, message)
                controlers = self.getChildren()
                ct = controlers['FileDialog']
                message = Message({'action':'list',
                                   'items':feedback['items'],
                                   'StartItem':0,
                                   'MaxItem':self.getPageSize()})
                ct.messageFromParent(message)
                message = Message({'action':'arrowCursor', 'type':None})
                self.getParent().messageFromChild(self, message)
              else:
                self.getWidget().getTree().showTree(feedback['items'], parentItem)



  #############################################################################
  def _on_item_clicked(self, parentItem):
    """handles the action generated by clicking one of the tree node"""
    gLogger.debug('One item clicked')
    parentnode = parentItem.parent()
    if parentnode != None:
      parent = parentnode.getUserObject()
      controlers = self.getChildren()
      if parent.has_key('level') and parent.has_key('showFiles'):
        path = parent['fullpath']
        message = Message({'action':'expande', 'node':path, 'StartItem':0, 'MaxItem':self.getPageSize()})
        self.__pagesize += self.__offset
        feedback = self.getParent().messageFromChild(self, message)
        if feedback.action() == 'showNode':
          message = Message({'action':'waitCursor', 'type':None})
          self.getParent().messageFromChild(self, message)
          message = Message({'action':'list', 'items':feedback['items']})
          ct = controlers['FileDialog']
          ct.messageFromParent(message)
          message = Message({'action':'arrowCursor', 'type':None})
          self.getParent().messageFromChild(self, message)

  #############################################################################
  def __openPath(self, name, item):
    """open a specific sub tree"""
    if item != None:
      for i in range(item.childCount()):
        node = item.child(i)
        userObject = node.getUserObject()
        if userObject != None:
          if userObject['name'] == name:
            self.getWidget().getTree().expandItem(node)
            return node
    else:
      message = 'The path is wrong!'
      gLogger.error(message)
      QMessageBox.critical(self.getWidget(), "ERROR", message, QMessageBox.Ok)

  def browsePath(self, path):
    """browse the path"""
    info = path.split(':/')
    if len(info) > 1:
      prefix = info[0]
      path = info[1]
      message = Message({'action':'BookmarksPrefices'})
      feedback = self.getParent().messageFromChild(self, message)
      if feedback['OK']:
        curentprefix = feedback['Value']
        if curentprefix != prefix:
          answer = QMessageBox.information(self.getWidget(),
                                           "Different query or query type",
                                           " Do you want to refresh the tree? If yes, Your selection will lost!!!",
                                           QMessageBox.Yes, QMessageBox.No)
          if answer == QMessageBox.Yes:
            qType = prefix.split('+')
            if qType[0] == 'sim':
              self.configButton()
              self.getWidget().setSimRadioButton()
            elif qType[0] == 'evt':
              self.eventTypeButton()
              self.getWidget().setEvtButton()

            elif qType[0] == 'prod':
              self.productionRadioButton()
              self.getWidget().setProdButton()

            elif qType[0] == 'run':
              self.runRadioButton()
              self.getWidget().setRunButton()

            if qType[1] == 'std':
              self.standardQuery()
              self.getWidget().setStandardQueryValue()

            elif qType[1] == 'adv':
              self.advancedQuery()
              self.getWidget().setAdvancedQueryValue()

            self.__openTreeNodes(path)
          elif answer == QMessageBox.No:
            return
        else:
          self.__openTreeNodes(path)
      else:
        QMessageBox.critical(self.getWidget(), "Error", str(feedback['Message']), QMessageBox.Ok)
        gLogger.error(feedback['Message'])

    else:
      QMessageBox.critical(self.getWidget(), "Error", 'Wrong path!', QMessageBox.Ok)
      gLogger.error('Wrong path!')

  #############################################################################
  def __openTreeNodes(self, path):
    """opens a node"""
    npath = path.split('/')
    node = self.getWidget().getTree().findItems(npath[1], Qt.MatchExactly, 0)
    parentItem = node[0]
    self.getWidget().getTree().expandItem(parentItem)
    node = '/' + npath[1]
    for i in range(2, len(npath)):
      if npath[i] != '':
        node = npath[i]
        item = self.__openPath(node, parentItem)
        parentItem = item

    if parentItem != None and parentItem.childCount() > 0:
      node = parentItem.child(0) # this two line open the File Dialog window
      self._on_item_clicked(node)

  #############################################################################
  def on_itemDuble_clicked(self, parentItem):
    """handles the action of the double click"""
    self._on_item_clicked(parentItem)


  #############################################################################
  def moreInformations(self):
    """handles the more information action"""
    currentItem = self.getWidget().getTree().getCurrentItem()
    node = currentItem.getUserObject()

    controlers = self.getChildren()
    ct = controlers['InfoDialog']

    if node != None:
      if 'level' in node and node['level'] == 'Processing Pass':
        ctproc = controlers['ProcessingPassDialog']
        message = Message({'action':'list', 'items':node})
        ctproc.messageFromParent(message)
      elif 'level' in node and node['level'] == 'FileTypes':
        ctproc = controlers['ProcessingPassDialog']
        message = Message({'action':'deatiledList', 'items':node})
        ctproc.messageFromParent(message)
      elif node.has_key('level') and node['level'] == 'Production(s)/Run(s)':
        message = Message({'action':'ProductionInformations', 'production':node['name']})
        feedback = self.getParent().messageFromChild(self, message)
        if feedback != None:
          message = Message({'action':'showprocessingpass', 'items':feedback})
          ctproc = controlers['ProcessingPassDialog']
          ctproc.messageFromParent(message)
      elif type(node) != types.DictType and node.expandable() :
        message = Message({'action':'list', 'items':node})
        ct.messageFromParent(message)
    else:
      QMessageBox.critical(self.getWidget(),
                           "Info",
                           'Please right click on the folder to see more information!',
                           QMessageBox.Ok)

  #############################################################################
  def bookmarks(self):
    """handles the bookmarks action"""
    currentItem = self.getWidget().getTree().getCurrentItem()
    curent = currentItem
    path = ''
    nodes = [ curent ]
    while curent != None:
      curent = curent.parent()
      nodes += [curent]

    nodes.reverse()
    for i in  nodes:
      if i != None:
        node = i.getUserObject()
        if node != None and node.has_key('name'):
          path += '/' + node['name']

    message = Message({'action':'BookmarksPrefices'})
    feedback = self.getParent().messageFromChild(self, message)
    if not feedback['OK']:
      gLogger.error(feedback['Message'])
    else:
      values = feedback['Value']
      controlers = self.getChildren()
      ct = controlers['Bookmarks']
      fullpath = values + ':/' + path
      message = Message({'action':'showValues', 'paths':{'Title':path, 'Path':fullpath}})
      feedback = ct.messageFromParent(message)
      if not feedback['OK']:
        gLogger.error(feedback['Message'])

  #############################################################################
  def getPageSize(self):
    """returns the size of the File dialog window"""
    value = self.getWidget().getPageSize()
    if type(str(value)) == types.StringType:
      if value == 'ALL':
        self.__offset = 0
      elif type(int(value)) == types.IntType:
        self.__offset = int(value)
    return self.__offset


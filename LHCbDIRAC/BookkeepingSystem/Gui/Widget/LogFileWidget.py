########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"


import sys

from PyQt4                                                           import QtCore, QtGui
from HttpWidget_ui                                                   import Ui_HttpWidget
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerLogInfo          import ControlerLogInfo

class LogFileWidget(QtGui.QDialog):
  
  def __init__(self, parent=None):
    super(LogFileWidget, self).__init__(parent)
    self.ui = Ui_HttpWidget()
    self.ui.setupUi(self)

        # set margins
    l = self.layout()
    l.setMargin(0)
    self.ui.horizontalLayout.setMargin(5)

    # set the default
    url = 'http://lhcb-logs.cern.ch/storage/lhcb/'
    self.ui.url.setText(url)

    # load page
    self.ui.webView.setUrl(QtCore.QUrl(url))

    self.__controler = ControlerLogInfo(self, parent.getControler())
    
    # history buttons:
    self.ui.back.setEnabled(False)
    self.ui.next.setEnabled(False)

    QtCore.QObject.connect(self.ui.back,QtCore.SIGNAL("clicked()"), self.back)
    QtCore.QObject.connect(self.ui.next,QtCore.SIGNAL("clicked()"), self.next)
    QtCore.QObject.connect(self.ui.url,QtCore.SIGNAL("returnPressed()"), self.url_changed)
    QtCore.QObject.connect(self.ui.webView,QtCore.SIGNAL("linkClicked (const QUrl&)"), self.link_clicked)
    QtCore.QObject.connect(self.ui.webView,QtCore.SIGNAL("urlChanged (const QUrl&)"), self.link_clicked)
    QtCore.QObject.connect(self.ui.webView,QtCore.SIGNAL("loadProgress (int)"), self.load_progress)
    QtCore.QObject.connect(self.ui.webView,QtCore.SIGNAL("titleChanged (const QString&)"), self.title_changed)
    QtCore.QObject.connect(self.ui.reload,QtCore.SIGNAL("clicked()"), self.reload_page)
    QtCore.QObject.connect(self.ui.stop,QtCore.SIGNAL("clicked()"), self.stop_page)

    QtCore.QMetaObject.connectSlotsByName(self)

  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def setUrl(self, url):
    self.ui.url.setText(url)
    self.reload_page()
  
  #############################################################################  
  def setUrlUsingStorage(self, url):
    newurl = 'http://lhcb-logs.cern.ch/storage'+url
    self.ui.url.setText(newurl)
    self.reload_page()
    
  #############################################################################  
  def url_changed(self):
    """
    Url have been changed by user
    """
    page = self.ui.webView.page()
    history = page.history()
    if history.canGoBack():
      self.ui.back.setEnabled(True)
    else:
      self.ui.back.setEnabled(False)
    if history.canGoForward():
      self.ui.next.setEnabled(True)
    else:
      self.ui.next.setEnabled(False)
  
    url = self.ui.url.text()
    self.ui.webView.setUrl(QtCore.QUrl(url))
                
  def stop_page(self):
    """
    Stop loading the page
    """
    self.ui.webView.stop()

  def title_changed(self, title):
    """
    Web page title changed - change the tab name
    """
    self.setWindowTitle(title)

  def reload_page(self):
    """
    Reload the web page
    """
    self.ui.webView.setUrl(QtCore.QUrl(self.ui.url.text()))

  def link_clicked(self, url):
    """
    Update the URL if a link on a web page is clicked
    """
    page = self.ui.webView.page()
    history = page.history()
    if history.canGoBack():
      self.ui.back.setEnabled(True)
    else:
      self.ui.back.setEnabled(False)
    if history.canGoForward():
      self.ui.next.setEnabled(True)
    else:
      self.ui.next.setEnabled(False)
  
    self.ui.url.setText(url.toString())

  def load_progress(self, load):
    """
    Page load progress
    """
    if load == 100:
      self.ui.stop.setEnabled(False)
    else:
      self.ui.stop.setEnabled(True)

  def back(self):
    """
    Back button clicked, go one page back
    """
    page = self.ui.webView.page()
    history = page.history()
    history.back()
    if history.canGoBack():
      self.ui.back.setEnabled(True)
    else:
      self.ui.back.setEnabled(False)

  def next(self):
    """
    Next button clicked, go to next page
    """
    page = self.ui.webView.page()
    history = page.history()
    history.forward()
    if history.canGoForward():
      self.ui.next.setEnabled(True)
    else:
      self.ui.next.setEnabled(False)


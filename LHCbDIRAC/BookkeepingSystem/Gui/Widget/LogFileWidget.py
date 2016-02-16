# pylint: skip-file

"""
Log widget
"""
########################################################################
# $Id: LogFileWidget.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################

__RCSID__ = "$Id: LogFileWidget.py 84842 2015-08-11 13:47:15Z fstagni $"


from PyQt4                                                               import QtCore, QtGui
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_HttpWidget                import Ui_HttpWidget
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerLogInfo          import ControlerLogInfo

class LogFileWidget(QtGui.QDialog):
  """
  LogFileWidget class
  """
  def __init__(self, parent=None):
    """initialize the widget"""
    QtGui.QDialog.__init__(self, parent)
    self.__httpWidget = Ui_HttpWidget()
    self.__httpWidget.setupUi(self)
    # set margins
    layout = self.layout()
    layout.setMargin(0)
    self.__httpWidget.horizontalLayout.setMargin(5)

    # set the default
    url = 'http://lhcb-logs.cern.ch/storage/lhcb/'
    self.__httpWidget.url.setText(url)

    # load page
    self.__httpWidget.webView.setUrl(QtCore.QUrl(url))

    self.__controler = ControlerLogInfo(self, parent.getControler())

    # history buttons:
    self.__httpWidget.back.setEnabled(False)
    self.__httpWidget.next.setEnabled(False)

    QtCore.QObject.connect(self.__httpWidget.back, QtCore.SIGNAL("clicked()"), self.back)
    QtCore.QObject.connect(self.__httpWidget.next, QtCore.SIGNAL("clicked()"), self.next)
    QtCore.QObject.connect(self.__httpWidget.url, QtCore.SIGNAL("returnPressed()"), self.url_changed)
    QtCore.QObject.connect(self.__httpWidget.webView, QtCore.SIGNAL("linkClicked (const QUrl&)"), self.link_clicked)
    QtCore.QObject.connect(self.__httpWidget.webView, QtCore.SIGNAL("urlChanged (const QUrl&)"), self.link_clicked)
    QtCore.QObject.connect(self.__httpWidget.webView, QtCore.SIGNAL("loadProgress (int)"), self.load_progress)
    QtCore.QObject.connect(self.__httpWidget.webView, QtCore.SIGNAL("titleChanged (const QString&)"), self.title_changed)
    QtCore.QObject.connect(self.__httpWidget.reload, QtCore.SIGNAL("clicked()"), self.reload_page)
    QtCore.QObject.connect(self.__httpWidget.stop, QtCore.SIGNAL("clicked()"), self.stop_page)

    QtCore.QMetaObject.connectSlotsByName(self)

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def setUrl(self, url):
    """sets the URL"""
    self.__httpWidget.url.setText(url)
    self.reload_page()

  #############################################################################
  def setUrlUsingStorage(self, url):
    """concatenate the URLs"""
    newurl = 'http://lhcb-logs.cern.ch/storage' + url
    self.__httpWidget.url.setText(newurl)
    self.reload_page()

  #############################################################################
  def url_changed(self):
    """
    Url have been changed by user
    """
    page = self.__httpWidget.webView.page()
    history = page.history()
    if history.canGoBack():
      self.__httpWidget.back.setEnabled(True)
    else:
      self.__httpWidget.back.setEnabled(False)
    if history.canGoForward():
      self.__httpWidget.next.setEnabled(True)
    else:
      self.__httpWidget.next.setEnabled(False)

    url = self.__httpWidget.url.text()
    self.__httpWidget.webView.setUrl(QtCore.QUrl(url))

  def stop_page(self):
    """
    Stop loading the page
    """
    self.__httpWidget.webView.stop()

  def title_changed(self, title):
    """
    Web page title changed - change the tab name
    """
    self.setWindowTitle(title)

  def reload_page(self):
    """
    Reload the web page
    """
    self.__httpWidget.webView.setUrl(QtCore.QUrl(self.__httpWidget.url.text()))

  def link_clicked(self, url):
    """
    Update the URL if a link on a web page is clicked
    """
    page = self.__httpWidget.webView.page()
    history = page.history()
    if history.canGoBack():
      self.__httpWidget.back.setEnabled(True)
    else:
      self.__httpWidget.back.setEnabled(False)
    if history.canGoForward():
      self.__httpWidget.next.setEnabled(True)
    else:
      self.__httpWidget.next.setEnabled(False)

    self.__httpWidget.url.setText(url.toString())

  def load_progress(self, load):
    """
    Page load progress
    """
    if load == 100:
      self.__httpWidget.stop.setEnabled(False)
    else:
      self.__httpWidget.stop.setEnabled(True)

  def back(self):
    """
    Back button clicked, go one page back
    """
    page = self.__httpWidget.webView.page()
    history = page.history()
    history.back()
    if history.canGoBack():
      self.__httpWidget.back.setEnabled(True)
    else:
      self.__httpWidget.back.setEnabled(False)

  def next(self):
    """
    Next button clicked, go to next page
    """
    page = self.__httpWidget.webView.page()
    history = page.history()
    history.forward()
    if history.canGoForward():
      self.__httpWidget.next.setEnabled(True)
    else:
      self.__httpWidget.next.setEnabled(False)


########################################################################
# $Id$
########################################################################
"""
Help class
"""
from DIRAC                                                               import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

############################################################################# 
class Help:
  
  ############################################################################# 
  def __init__(self):
    pass
  
  ############################################################################# 
  def helpConfig(self, treeLevels):
    if treeLevels==-1:
      print "-------------------------------------"
      print "| Please use the following comand:   |"
      print "| client.list()                      |"
      print "--------------------------------------"
    elif treeLevels==0:
      print "-----------------------------------------"
      print "| Please choose one configuration!       |"
      print "| For example:                           |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5') |"
      print "------------------------------------------"
      
    elif treeLevels==1:
      print "-------------------------------------------------------"
      print "| Please choose one event type!                       |"
      print "| For example:                                        |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010') |"
      print "-------------------------------------------------------"
      
    elif treeLevels==2:
      print "-----------------------------------------------------------------"
      print "| Please choose one production!                                 |"
      print "| For example:                                                  |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010/PROD_1933') |"
      print "-----------------------------------------------------------------"
    
    elif treeLevels==3:
      print "---------------------------------------------------------------------------------------------------------------"
      print "| Please choose one file type!                                                                                 |"
      print "| For example:                                                                                                 |"
      print "| client.list('/CFG_DC06 phys-v3-lumi5/EVT_10000010/PROD_1933/FTY_RDST Brunel v30r17')                         |"
      print "----------------------------------------------------------------------------------------------------------------"

  
  ############################################################################# 
  def helpProcessing(self, treeLevels):
    if treeLevels==-1:
      print "-------------------------------------"
      print "| Please use the following comand:   |"
      print "| client.list()                      |"
      print "--------------------------------------"
    elif treeLevels==0:
      print "-----------------------------------------"
      print "| Please choose one Processing Pass!     |"
      print "| For example:                           |"
      print "| client.list('/PPA_Pass342')            |"
      print "------------------------------------------"

    elif treeLevels==1:
      print "-------------------------------------------------------"
      print "| Please choose one production!                       |"
      print "| For example:                                        |"
      print "| client.list('/PPA_Pass342/PRO_1858')                |"
      print "-------------------------------------------------------"

    elif treeLevels==2:
      print "-----------------------------------------------------------------"
      print "| Please choise one event type!                                 |"
      print "| For example:                                                  |"
      print "| client.list('/PPA_Pass342/PRO_1858/EVT_10000000')             |"
      print "-----------------------------------------------------------------"

    elif treeLevels==3:
      print "-----------------------------------------------------------------"
      print "| Please choose one file type!                                  |"
      print "| For example:                                                  |"
      print "| client.list('/PPA_Pass342/PRO_1858/EVT_10000000/FTY_RDST')    |"
      print "-----------------------------------------------------------------"

  ############################################################################# 
  def helpEventType(self, treeLevels):
    gLogger.warn("Not Implemented!")

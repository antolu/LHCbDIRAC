''' LHCbDIRAC.ResourceStatusSystem.Agent.ShiftEmail

  __productionBody__
  getBodyEmail
  
'''

__RCSID__ = "$Id$"

#...............................................................................

__productionBody__ = '''Dear %s, 

this is an (automatic) mail to welcome you on the grid operations shifts. In order to facilitate your shift activities we wanted to provide you some pointers where you could find more information about shifts, the activities and your duties during this period. 


http://lhcb-shifters.web.cern.ch/

A web page which is currently growing and finally should contain all information that is needed for shift activities, e.g. current productions. We would especially ask you to keep the "Current Issues" section in the "Dashboard" page up to date, this section shall contain any persistent problems and everything being solved should be removed. It's also your entry point to see if any issues that you may find are already known or not. 

http://lhcb-web-dirac.cern.ch/DIRAC/LHCb-Production/visitor/View/Presenter/display

The anonymous entry to the Dirac portal, this is the place where we are providing shared plots about the major ongoing activities and workflows (data processing, data transfer, pilots, etc). The overview pages and the individual plots will be described in the shifters web page (see above) under the "Dirac Plots" entry. 

https://lblogbook.cern.ch/Operations/

The logbook for LHCb operations, where all activities concerning offline data processing and transfer are being logg'ed. 


Many more tools are being mentioned in the lhcb-shifters web page. Please check the Tools section for more information. 

Today your GEOC is %s. As this mail is not being resent please check if the GEOC is changing during your shift period. 

If you have any comments, criticism, suggestions for improvements about the organization of grid shifts please feel free to come back to Stefan.Roiser@cern.ch or Joel.Closier@cern.ch any time. 

Please don't forget to attend the 11.15 LHCb Computing Operations Meeting to provide your shift report and to eLog a shift report at the end of each day. 

Best regards and happy shifting

  Joel & Stefan
  
'''  

#...............................................................................

def getBodyEmail( role ):
  '''
    Returns a body for the email if provided a proper role.
  '''

  if role == 'Production':
    return __productionBody__
  else:
    return None

#...............................................................................
#EOF

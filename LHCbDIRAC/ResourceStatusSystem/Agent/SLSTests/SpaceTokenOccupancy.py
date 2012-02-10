################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gLogger
from DIRAC.ResourceStatusSystem.Utilities                   import CS
from LHCbDIRAC.ResourceStatusSystem.Agent.SLSTests.TestBase import TestBase

import lcg_util, time

class TestModule( TestBase ):
  
  def launchTest( self ):
    '''
      Main test method.
    '''
    
    self.SEs         = CS.getSpaceTokenEndpoints()
    self.spaceTokens = CS.getSpaceTokens() 

    for site in self.SEs:
      for spaceToken in self.spaceTokens:
        
        try:
        
          self.__launchTest( site, spaceToken )
        
        except Exception, e:
          gLogger.exception( 'StorageTokenOccupancy for %s %s.\n %s' % ( site, spaceToken, e ) )     

  def __launchTest( self, site, spaceToken ):

    total, guaranteed, free, availability = 0, 0, 0, 0

    url              = self.SEs[ site ][ 'Endpoint' ]
    validityduration = 'PT0M'   

    # get space tokens associated to a space token description, and their metadata
    answer = lcg_util.lcg_stmd( spaceToken, url, True, 0 )

    print answer

    if answer[ 0 ] == 0:
      output           = answer[1][0]
      total            = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
      guaranteed       = float( output[ 'guaranteedsize' ] ) / 1e12
      free             = float( output[ 'unusedsize' ] ) / 1e12
      availability     = 100 if free > 4 else ( free * 100 / total if total != 0 else 0 )
      #validity     = self.getTestOption( "validity" )
      validityduration = self.testConfig[ 'validityduration' ]
    else:
      _msg = 'StorageTokenOccupancy: problem with lcg_util.lcg_stmd( "%s","%s",True,0 ) = (%d, %s)'
      gLogger.info(  _msg % ( spaceToken, url, answer[0], answer[1] ) )
      gLogger.info( str( answer ) )

    ## XML generation

    xmlReport = []
    xmlReport.append( { 'tag' : 'id', 'nodes' : '%s_%s' % ( site, spaceToken ) } )
    xmlReport.append( { 'tag' : 'availability', 'nodes' : availability } )
    
    thresholdNodes = []
    for t,v in self.testConfig[ 'thresholds' ].items():
      thresholdNodes.append( { 'tag' : 'threshold', 'attrs' : [ ( 'level', t ) ], 'nodes' : v } )
    
    xmlReport.append( { 'tag' : 'availabilitythresholds', 'nodes' : thresholdNodes } )
    
    xmlReport.append( { 'tag' : 'availabilityinfo' , 'nodes' : 'Free=%s Total=%s' % ( free, total ) } )
    xmlReport.append( { 'tag' : 'availabilitydesc' , 'nodes' : self.testConfig[ 'availabilitydesc' ] } )
    xmlReport.append( { 'tag' : 'refreshperiod'    , 'nodes' : self.testConfig[ 'refreshperiod' ] } )
    xmlReport.append( { 'tag' : 'validityduration' , 'nodes' : validityduration } )

    dataNodes = []
    grpNodes  = []
    
    grpNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Consumed' ) ], 'nodes' : total - free } ) 
    grpNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Capacity' ) ], 'nodes' : total } )
    grp  = { 'tag' : 'grp', 'attrs' : [ ( 'name', 'Space occupancy' ) ], 'nodes' : grpNodes }
    
    dataNodes.append( grp )
    
    dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Free space' ) ], 'nodes' : free } )
    dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Occupied space' ) ], 'nodes' : total - free } )
    dataNodes.append( { 'tag' : 'numericvalue', 'attrs' : [ ( 'name', 'Total space' ) ], 'nodes' : total } )
    dataNodes.append( { 'tag' : 'textvalue', 'nodes' : 'Storage space for the specific space token' } )
    
    xmlReport.append( { 'tag' : 'data', 'nodes' : dataNodes } )
        
    xmlReport.append( { 'tag' : 'timestamp', 'nodes' : time.strftime( "%Y-%m-%dT%H:%M:%S" ) })
    
    self.writeXml( xmlList = xmlReport )
#
#    Utils.unpack(insert_slsstorage(Site=site, Token=st, Availability=availability,
#                      RefreshPeriod="PT27M", ValidityDuration=validity,
#                      TotalSpace=int(total), GuaranteedSpace=int(guaranteed),
#                      FreeSpace=int(free)))
#
#    xmlfile = open(self.xmlPath + site + "_" + st + ".xml", "w")
#    try:
#      xmlfile.write(doc.toxml())
#    finally:
#      xmlfile.close()

    # Send notifications
    # pledged = get_pledged_value_for_token(site, st)
    # if not fake and total+1 < pledged:
    #   gLogger.info("%s/%s: pledged = %f, total = %f, sending mail to site..." % (site, st, pledged, total))
    #   send_mail_to_site(site, st, pledged, total)

    # Dashboard
#    dbfile = open(self.xmlPath + site + "_" + st  + "_space_monitor", "w")
#    try:
#      dbfile.write(st + ' ' + str(total) + ' ' + str(guaranteed) + ' ' + str(free) + '\n')
#    finally:
#      dbfile.close()

    gLogger.info("SpaceTokenOccupancyTest: %s/%s done." % ( site, st ) )


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
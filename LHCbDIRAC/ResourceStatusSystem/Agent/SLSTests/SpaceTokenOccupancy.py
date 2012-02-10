################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC                                                  import gLogger
from DIRAC.ResourceStatusSystem.Utilities                   import CS
from LHCbDIRAC.ResourceStatusSystem.Agent.SLSTests.TestBase import TestBase

import lcg_util

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
            
#        print site, spaceToken


  def __launchTest( self, site, spaceToken ):

    total, guaranteed, free, availability = 0, 0, 0, 0

    url          = self.SEs[ site ][ 'Endpoint' ]
    validity     = 'PT0M'   

    # get space tokens associated to a space token description, and their metadata
    answer = lcg_util.lcg_stmd( spaceToken, url, True, 0 )

    print output

    if answer[ 0 ] == 0:
      output       = answer[1][0]
      total        = float( output[ 'totalsize' ] ) / 1e12 # Bytes to Terabytes
      guaranteed   = float( output[ 'guaranteedsize' ] ) / 1e12
      free         = float( output[ 'unusedsize' ] ) / 1e12
      availability = 100 if free > 4 else ( free * 100 / total if total != 0 else 0 )
      #validity     = self.getTestOption( "validity" )
      validity     = self.testConfig[ 'validity' ]
    else:
      gLogger.info("StorageSpace: problew with lcg_util:\ lcg_util.lcg_stmd('%s', '%s', True, 0) = (%d, %s)" % (st, url, answer[0], answer[1]))
      gLogger.info( str( answer ) )

#    doc = gen_xml_stub()
#    xml_append(doc, "id", site + "_" + st)
#    xml_append(doc, "availability", availability)
#    elt = xml_append(doc, "availabilitythresholds")
#    xml_append(doc, "threshold", value_=self.getTestOption("Thresholds/available"), elt_=elt, level="available")
#    xml_append(doc, "threshold", value_=self.getTestOption("Thresholds/affected"), elt_=elt, level="affected")
#    xml_append(doc, "threshold", value_=self.getTestOption("Thresholds/degraded"), elt_=elt, level="degraded")
#    xml_append(doc, "availabilityinfo", "Free="+str(free)+" Total="+str(total))
#    xml_append(doc, "availabilitydesc", self.getTestValue("availabilitydesc"))
#    xml_append(doc, "refreshperiod", self.getTestValue("refreshperiod"))
#    xml_append(doc, "validityduration", validity)
#    elt = xml_append(doc, "data")
#    elt2 = xml_append(doc, "grp", name="Space occupancy", elt_=elt)
#    xml_append(doc, "numericvalue", value_=str(total-free), elt_=elt2, name="Consumed")
#    xml_append(doc, "numericvalue", value_=str(total), elt_=elt2, name="Capacity")
#    xml_append(doc, "numericvalue", value_=str(free), elt_=elt, name="Free space")
#    xml_append(doc, "numericvalue", value_=str(total-free), elt_=elt, name="Occupied space")
#    xml_append(doc, "numericvalue", value_=str(total), elt_=elt, name="Total space")
#    xml_append(doc, "textvalue", "Storage space for the specific space token", elt_=elt)
#    xml_append(doc, "timestamp", time.strftime("%Y-%m-%dT%H:%M:%S"))
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

    gLogger.info("SpaceTokenOccupancyTest: %s/%s done." % (site, st))


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
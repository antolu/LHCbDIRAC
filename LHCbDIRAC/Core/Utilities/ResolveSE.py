""" Resolve SE takes the workflow SE description and returns the list
    of destination storage elements for uploading an output file.
"""

from random import shuffle

from DIRAC                              import gLogger, gConfig
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers

__RCSID__ = "$Id$"

def getDestinationSEList( outputSE, site, outputmode = 'Any', run = None ):
  """ Evaluate the output SE list from a workflow and return the concrete list
      of SEs to upload output data.
  """
  if outputmode.lower() not in ( 'any', 'local', 'run' ):
    raise RuntimeError( "Unexpected outputmode" )

  if outputmode.lower() == 'run':
    gLogger.verbose( "Output mode set to 'run', thus ignoring site parameter" )
    if not run:
      raise RuntimeError( "Expected runNumber" )
    try:
      run = long( run )
    except ValueError, ve:
      raise RuntimeError( "Expected runNumber as a number: %s" % ve )

    gLogger.debug( "RunNumber = %d" % run )
    from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    runDestination = TransformationClient().getDestinationForRun( run )
    if not runDestination['OK']:
      gLogger.error( "Issue getting destinationForRun", runDestination['Message'] )
      raise RuntimeError( "Issue getting destinationForRun" )
    site = runDestination['Value'][run]
    outputmode = 'Local'

  localMode = ( outputmode.lower() == "local" )
  # Add output SE defined in the job description
  gLogger.info( 'Resolving workflow output, SE description: %s' % outputSE )

  # Check if the SE is defined explicitly for the site
  prefix = site.split( '.' )[0]
  country = site.split( '.' )[-1]
  # Concrete SE name
  result = gConfig.getOptions( '/Resources/StorageElements/' + outputSE )
  if result['OK']:
    gLogger.info( 'Found concrete SE %s' % outputSE )
    return [outputSE]
  # There is an alias defined for this Site
  aliasSE = gConfig.getValue( '/Resources/Sites/%s/%s/AssociatedSEs/%s' % ( prefix, site, outputSE ), [] )
  if aliasSE:
    gLogger.info( "Found associated SE %s for site %s" % ( aliasSE, site ) )
    return aliasSE

  # Assume outputSE is a StorageElementGroup that should be defines as such
  groupSEs = gConfig.getValue( '/Resources/StorageElementGroups/' + outputSE, [] )
  if not groupSEs:
    raise RuntimeError( "Failed to resolve SE group " + outputSE )
  shuffle( groupSEs )
  gLogger.verbose( "Group SE list is: %s" % ( groupSEs ) )

  # If "local" is requested get a local SE in the group
  dmsHelper = DMSHelpers()
  if localMode:
    # At this point we haven't found a local SE yet
    # outputSE is therefore a StorageElementGroup which may be defined for an associated country
    # Check recursively for country associated SE
    # Try and find a site in the associated countries that has a local SE in the groupSE
    tier1 = site
    while True:
      # Get list of local SEs; it may not exist if the site has no local SEs
      localSEs = dmsHelper.getSEsForSite( tier1 ).get( 'Value', [] )
      gLogger.verbose( "Local SE list to %s is: %s" % ( tier1, localSEs ) )
      # Find local SEs in the group
      localGroupSEs = [se for se in groupSEs if se in localSEs]
      if localGroupSEs:
        gLogger.info( "Found eligible local SE at %s: %s" % ( tier1, ','.join( localGroupSEs ) ) )
        return localGroupSEs
      # Then look whether there is an associated SE matching
      # gLogger.verbose( '/Resources/Countries/%s/AssociatedSEs/%s' % ( country, outputSE ) )
      aliasSE = gConfig.getValue( '/Resources/Countries/%s/AssociatedSEs/%s' % ( country, outputSE ), [] )
      if aliasSE:
        # Found: return
        gLogger.info( "Found associated SE for site %s in country %s: %s" % ( site, country, ','.join( aliasSE ) ) )
        return aliasSE
      # Check if that country has an associated Tier1 that is not the same
      gLogger.verbose( "/Resources/Countries/%s/Tier1" % country )
      newTier1 = gConfig.getValue( "/Resources/Countries/%s/Tier1" % country, None )
      if newTier1 and newTier1 != tier1:
        tier1 = newTier1
      else:
        # No or same tier1: try and move to an associated country
        gLogger.verbose( "/Resources/Countries/%s/AssignedTo" % country )
        country = gConfig.getValue( "/Resources/Countries/%s/AssignedTo" % country, None )
        if not country:
          # No point continuing
          break

    # Here we haven't found any local or associated SE
    raise RuntimeError( "No local SE was found at %s for %s" % ( site, outputSE ) )

  # Mode is "any": make sure that local SEs are passed first
  localSEs = dmsHelper.getSEsForSite( site ).get( 'Value', [] )
  listOfSEs = [se for se in groupSEs if se in localSEs] + [se for se in groupSEs if se not in localSEs]
  gLogger.verbose( 'Found unique SEs: %s' % ( listOfSEs ) )
  return listOfSEs

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

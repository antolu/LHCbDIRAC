""" Resolve SE takes the workflow SE description and returns the list
    of destination storage elements for uploading an output file.
"""

# FIXME: this might go/use the Resources CS helper once it becomes a class (v7r0...)

from random import shuffle

from DIRAC                              import gLogger, gConfig
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.List          import uniqueElements

def getDestinationSEList( outputSE, site, outputmode = 'Any' ):
  """ Evaluate the output SE list from a workflow and return the concrete list
      of SEs to upload output data.
  """
  # Add output SE defined in the job description
  gLogger.info( 'Resolving workflow output SE description: %s' % outputSE )

  # Check if the SE is defined explicitly for the site
  prefix = site.split( '.' )[0]
  country = site.split( '.' )[-1]
  # Concrete SE name
  result = gConfig.getOptions( '/Resources/StorageElements/' + outputSE )
  if result['OK']:
    gLogger.info( 'Found concrete SE %s' % outputSE )
    return [outputSE]
  # There is an alias defined for this Site
  alias_se = gConfig.getValue( '/Resources/Sites/%s/%s/AssociatedSEs/%s' % ( prefix, site, outputSE ), [] )
  if alias_se:
    gLogger.info( 'Found associated SE for site %s' % ( alias_se ) )
    return alias_se

  localSEs = getSEsForSite( site )
  if not localSEs['OK']:
    raise RuntimeError( localSEs['Message'] )
  localSEs = localSEs['Value']
  gLogger.verbose( "Local SE list is: %s" % ( localSEs ) )

  groupSEs = gConfig.getValue( '/Resources/StorageElementGroups/' + outputSE, [] )
  if not groupSEs:
    raise RuntimeError( "Failed to resolve SE " + outputSE )
  shuffle( groupSEs )
  gLogger.verbose( "Group SE list is: %s" % ( groupSEs ) )

  if outputmode.lower() == "local":
    for se in localSEs:
      if se in groupSEs:
        gLogger.info( 'Found eligible local SE: %s' % ( se ) )
        return [se]

    # check if country is already one with associated SEs
    section = '/Resources/Countries/%s/AssociatedSEs/%s' % ( country, outputSE )
    associatedSE = gConfig.getValue( section, '' )
    if associatedSE:
      gLogger.info( 'Found associated SE %s in %s' % ( associatedSE, section ) )
      return [shuffle( associatedSE )]

    # Final check for country associated SE
    count = 0
    assignedCountry = country
    while count < 10:
      gLogger.verbose( 'Loop count = %s' % ( count ) )
      gLogger.verbose( "/Resources/Countries/%s/AssignedTo" % assignedCountry )
      opt = gConfig.getOption( "/Resources/Countries/%s/AssignedTo" % assignedCountry )
      if opt['OK'] and opt['Value']:
        assignedCountry = opt['Value']
        gLogger.verbose( '/Resources/Countries/%s/AssociatedSEs' % assignedCountry )
        assocCheck = gConfig.getOption( '/Resources/Countries/%s/AssociatedSEs' % assignedCountry )
        if assocCheck['OK'] and assocCheck['Value']:
          break
      count += 1

    section = '/Resources/Countries/%s/AssociatedSEs/%s' % ( assignedCountry, outputSE )
    alias_se = gConfig.getValue( section, [] )
    if alias_se:
      gLogger.info( 'Found alias SE for site: %s' % alias_se )
      return alias_se
    else:
      raise RuntimeError( 'Could not establish alias SE for country %s from section: %s' % ( country, section ) )

  # For collective Any and All modes return the whole group

  # Make sure that local SEs are passing first
  newSEList = []
  for se in groupSEs:
    if se in localSEs:
      newSEList.append( se )
  listOfSEs = uniqueElements( newSEList + groupSEs )
  gLogger.verbose( 'Found unique SEs: %s' % ( listOfSEs ) )
  return listOfSEs

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#

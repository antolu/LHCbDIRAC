from DIRAC import S_OK, S_ERROR, gLogger, gConfig

def resolveSE(site,outputSE,outputmode):
  """ Evaluate the output SE list
  """
  
  # Concrete SE name
  result = gConfig.getOptions('/Resources/StorageElements/'+outputSE)
  if result['OK']:
    return S_OK([outputSE])

  siteprefix,sitename,sitecountry = site.split('.')  

  # There is an alias defined for this Site
  alias_se = gConfig.getValue('/Resources/Sites/%s/%s/AssociatedSEs/%s' % (siteprefix,site,outputSE))
  if alias_se:
    return S_OK([alias_se])

  assignedCountry = sitecountry
  # There is an country alias defined for this Site
  alias_country = gConfig.getValue('/Resources/Sites/%s/%s/AssignedTo' % (siteprefix,site))
  if alias_country:
    assignedCountry = alias_country
    
  if outputmode == "Local":

    # Country associated SE
    count = 0
    while count<10:
      opt = gConfig.getOption("/Resources/Countries/%s/AssignedTo" %assignedCountry)
      if opt['OK']:
        assignedCountry = opt['Value']
        count += 1
      else:
      	break

    assocCheck = gConfig.getOption('/Resources/Countries/%s/AssociatedSEs/%s' %(assignedCountry,outputSE) )
    if assocCheck['OK']:
      return S_OK([assocCheck['Value']])
    else:
      return S_ERROR('Could not determine associated SE %s list for %s' %(outputSE,sitecountry))

  else:
  
    #For All or Any mode return the whole group
    groupSEs = gConfig.getValue('/Resources/StorageElementGroups/'+outputSE)
    if not groupSEs:
      return S_ERROR('Failed to resolve SE '+outputSE)
    else:
      return S_OK(groupSEs)

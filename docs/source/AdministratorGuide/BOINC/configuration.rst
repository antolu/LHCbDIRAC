===========================
Configuration of the system
===========================

There are in facts two distinct configurations:
  * The configuration used by the services and agents running on the gateway
  * The configuration used by the jobs

 Template files are available for you to use.

************************************
Configuration of services and agents
************************************

This configuration corresponds to the local configuration of the gateway as a vobox: the traditional dirac.cfg.
An example of such file can be found :download:`here <cfgTemplates/dirac.cfg>`.

First of all, because the components on the gateway will interact with the real system, this configuration file has to refer to the real CS and the setup used should be the same as the one the jobs will run it (so in fact, your production setup normally).

Systems
======

Even if all the services and agents definition could be left in the central CS if standard, we rather recommend to redefine them locally to have a self contained configuration file. For a list of components, please see :ref:`_listOfBOINCComponents`.

We strongly recommend to define a  database section  totally different from the production one.

As for the URLs, you do not need to define most of them as the calls should go directly to the real system, however there are a few exceptions:

  * *Framework/BoincProxyManager*: This URL should point at your local proxy manager. The reason is that the jobs will contact the WMSSecureGW to get a proxy. And this gateway will in fact query *Framework/BoincProxyManager* to retrieve the MrBoinc Proxy, and not *Framework/ProxyManager* not to leak any real proxy.
  * *RequestManagement/ReqManager*: This URL should point at your local ReqManager. The reason is that any request that the job will try to put will go through the gateway, which will modify it slightly (see REFERENCE TO REQUEST MODIFICATION), and then forward it to the local ReqManager, to be executed by the local RequestExecutingAgent. No request from a BOINC Job should ever reach the central system.
  * *RequestManagement/ReqProxyURLs*: this needs to be redefined to the local ReqProxy, otherwise the requests might enter the real system if they go through a central ReqProxy.


RequestExecutingAgent
---------------------

There is a special Operation to define in the RequestExecutingAgent: `WMSSecureOutputData`. It can point to any module you like. This Operation will be inserted in the first position whenever a job puts a Request. Typically, it is a safety operation, where you can check anything you like (provenance, nature of the output data, etc).


StorageElementProxy
---------------------

The StorageElementProxy will try getting the MrBoinc proxy when the job asks for the input sandbox. So for this service, we need to redefine the ProxyManager as pointing to the WMSSecureGW.

ReqProxy
---------------------

The ReqProxy must go through the WMSSecureGW in order to upload the request with the right ownerDN otherwise it will come up with MrBoincUser. So for this service, we need to redefine the ReqManager as pointing to the WMSSecureGW.


Operations
==========

To make sure to override anything that could be defined in the central CS, we need to put it in the setup specific section, not in the `Defaults` one.

* `BoincShifter/ProductionManager`: This is used by the WMSSecureGW to reassign the ownership of the requests coming from BOINC jobs. This will hopefully disappear in a future version.
* `BoincShifter/BoincShifter`: This is used by the WMSSecureGW to return the MrBoinc user proxy. The user and group should match what you define in Operations


Resources
=========

StorageElements
---------------

Here you need to only define BOINC-SE as your local storage element. Technically, you could avoid it because it must be defined anyway in the real CS. All the access must be set to Active.

Registry
--------

The DefaultGroup should be boinc_users.

Users
^^^^^

Only MrBoinc needs to be defined. The DN is the one of the user certificate you self generated (see :ref:`_mrBoincCert`)

CAUTION: not sure now, probably need to redefine all the shifter users.

Groups
^^^^^^

The groups are not merged with the central CS, but replaced.
You should define a boinc_users group, with NormalUser as property, and add only MrBoinc in it.


Hosts
^^^^^^
MrBoincHost should be defined as the certificate shipped with the VM image

**************************
Configuration for the jobs
**************************

This configuration is served to the jobs by the Configuration server running on the gateway.
It basically just defines what the job needs in order to run.

The setup used should be the same as the real one (like LHCb-Certification), but you have to redefine the setup as using a different instance for each system (like boincInstance)

Systems
=======

We only define the URLs, and all of them must point toe the WMSSecureGW.
There is currently one exception: the DataStore, which is not handle by the WMSSecureGW.

LocalSite
=========

This is your BOINC site name. It has to be consistend with what goes in the central CS (see bellow)

Operations
==========


CAUTION: check which protocol really need modification
* DataManagement: The protocol lists (RegistrationProtocols, ThirdPartyProtocols, WriteProtocols, AccessProtocols) need to be redefined in order to include 'proxy'
* ResourceStatus/Config: disable RSS all together.
* Services/Catalogs: define the catalogs you want to use.

Resources
=========

* FileCatalog: define the catalogs you want to use.
* Sites: only the BOINC site is needed
* StorageElement: all the SEs a job might need to write to or read from. Most of the definition can be fake, but they need to be there. What matters is that the configuration of BOINC-SE, which is used as failover, is correct.
* StorageElementGroups: all the groups that might potentially be used by your jobs. Important is to redefine the failover as BOINC-SE
* Computing: this gives the OS compatibility. Take it from your real system. Hopefuly, pilot3 will get ride of that.


**************************************
Configuration to put in the central CS
**************************************


.. _centralCSChanges:

Some changes are needed on your real system to have jobs flowing in BOINC.


In order to have the matcher send jobs to BOINC, you need to define the site just like in the BOINC-Conf.cfg::

    Resources
    {
      Sites
      {
        BOINC
        {
          BOINC.World.org
          {
            CE = Boinc-World-CE.org
            CEs
            {
              Boinc-World-CE.org
              {
                CEType = Boinc
                Queues
                {
                  MaxCPUTime = 100000
                  Boinc.World.Queue
                  {
                    MaxCPUTime = 100000
                  }
                }
              }
              # This special CE is for sending test jobs with a special tag
              Boinc-World-TestCE.org
              {
                CEType = Boinc
                Tag = BoincTestJobs
                Queues
                {
                  MaxCPUTime = 100000
                  Boinc.World.TestQueue
                  {
                    MaxCPUTime = 100000
                  }
                }
              }
            }
          }
        }
      }
    }

You might also want to define some running limits (typically, at the moment, you only want MC running there)::

  Operations
  {
    <Setup>
    {
      JobScheduling
      {
        RunningLimit
        {
          BOINC.World.org
          {
            JobType
            {
              User = 0
              MCSimulation = 500
            }
          }
        }
      }
    }
  }


You have to define the BOINC SE, just like it is in the gateway dirac.cfg, without the file protocol. The reason is that the REA of the gateway will have RSS enabled, so RSS must know this SE. Define an always banned RSS rule for it, so RSS does not bother trying to test it::

  Operations
  {
    <Setup>
    {
      ResourceStatus
      {
        Policies
        {
          Ban_BOINC_SE
          {
            policyType = AlwaysBanned
            matchParams
            {
              name = BOINC-SE
              statusType = ReadAccess
              statusType += WriteAccess
              statusType += CheckAccess
              elementType = StorageElement
            }
          }
        }
      }
    }
  }

======================
Installation procedure
======================

./install_site.sh install.cfg
dirac-install-mysql
create a file joelDB.sql which contains the line "use joelDB;"
and put it in a directoy in LHCbDIRAC structure called DB.

Tier1 VOBOX installation
------------------------

::

  mkdir -p <VODIR>/dirac/etc/grid-security/
  cp hostcert.pem hostkey.pem <VODIR>/dirac/etc/grid-security
  mkdir DIRAC; cd DIRAC
  wget -np http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/install_site.sh
  chmod 744 install_site.sh

Create the fine **install.cfg** with the typical attached lines.

::

   # This section determines which DIRAC components will installed and where
      LocalInstallation
      {
         # DIRAC release version
         Release = LHCb-v5r11
         # LCG software package version. Specify this option only if you need the gLite
         # UI installation
         LcgVer = 2009-08-13
         # Set this flag to yes if each DIRAC software update will be installed
         # in a separate directory, not overriding the previous ones
         UseVersionsDir = yes
         # The directory of the DIRAC software installation
         TargetPath = /opt/vobox/lhcb/dirac
         # DIRAC extensions to be installed
         Extensions = LHCb
         # Flag determining whether the Web Portal will be installed
         WebPortal = no
         # Site name
         SiteName = DIRAC.VOBOXLHCB.de
         # Setup name
         Setup = LHCb-Production
         # Default name of system instances
         InstanceName = Production
         # Flag to skip CA checks when talking to services
         SkipCAChecks = no
         # Flag to use the server certificates
         UseServerCertificate = yes
         # Configuration Server URL
         ConfigurationServer = dips://lhcbprod.pic.es:9135/Configuration/Server, dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
         IncludeAllServers = yes
         # Flag to set up the Configuration Server as Master
         ConfigurationMaster = no
         # Configuration Name
         ConfigurationName = LHCb-Prod
         # Name of the installation host (default: the current host )
         Host = lhcb-kit.gridka.de
         # DN of the host certificate (default: None )
         HostDN = /C=DE/O=GermanGrid/OU=KIT/CN=lhcbvobox/lhcb-kit.gridka.de
         # List of DIRAC Systems to be installed
         Systems = Configuration,Framework,RequestManagement
         # List of Services and Agents to be installed
         Services  = Configuration/Server
         Services += Framework/SystemAdministrator
         Services += RequestManagement/RequestManager
         Agents = RequestManagement/ZuziaAgent
      }


There is some customisation needed depending on the site where the VO-box is located.
run the following command **"install_site.sh install.cfg"**

Note :

1. The <VODIR> is typically "/opt/vobox/lhcb" in all the Tier-1 vo-boxes, except IN2P3 where it is "/vo/lhcb".
2. The sample "install.cfg" as tailored for GridKa is attached to this email.

For a simple update of an already installed vo-box, the you can use the DIRAC system admin CLI (dirac-admin-sysadmin-cli) to perform the update. You can also use the attached script (which does the same thing) to do the update.

The Tier-1 VO-box machines are :

::

  voboxlhcb.gridpp.rl.ac.uk (RAL)
  voboxlhcb.in2p3.fr (IN2P3)
  voboxlhcb.nikhef.nl (NL-T1)
  voboxlhcb.gridka.de (GRIDKA)
  voboxlhcb.pic.es  (PIC)
  ui01-lhcb.cr.cnaf.infn.it (CNAF)


ONLINE installation
-------------------
The ONLINE installation is done in **/sw/dirac** on the lbdirac.cern.ch machine.
The services which are running on this machine are :

::

  DataManagement/RegistrationAgent
  DataManagement/RemovalAgent
  DataManagement/TransferAgent
  RequestManagement/RequestManager


The current version is LHCb-v5r11p14 and the configuration for dirac.cfg is the following:

::

        LocalSite
        {
          EnableAgentMonitoring = yes
          Root = /sw/dirac
          Site = DIRAC.ONLINE.ch
        }
        DIRAC
        {
          Setup = LHCb-Production
          Configuration
          {
            Name = LHCb-Prod
            Servers =  dips://lhcb-conf-dirac.cern.ch:9135/Configuration/Server
          }
          Security
          {
            CertFile = /sw/dirac/etc/grid-security/hostcert.pem
            KeyFile = /sw/dirac/etc/grid-security/hostkey.pem
          }
          Setups
          {
            LHCb-Production
            {
              Accounting = Production
              Configuration = Production
              WorkloadManagement = Production
              ProductionManagement = Production
              Framework = Production
              Logging = Production
              DataManagement = Production
              RequestManagement = Production
              Monitoring = Production
              Bookkeeping = Production
            }
          }
        }
        Systems
        {
          RequestManagement
          {
            Production
            {
              URLs
              {
                localURL = dip://lbdirac.cern.ch:9199/RequestManagement/RequestManager
              }
              Services
              {
                RequestManager
                {
                  Port = 9199
                  Backend = file
                  Path = /sw/dirac/requestDB
                  MaxThreads = 100
                  LogOutputs = stdout, server
                }
              }
            }
          }
        }
        Resources
        {
          FileCatalogs
          {
            RAWIntegrity
            {
              AccessType = Read-Write
              Status = Active
            }
          }
          StorageElements
          {
            OnlineRunDB
            {
              StorageBackend = RunDB
              AccessProtocol.1
              {
                ProtocolName = LHCbOnline
                Access = local
                Protocol = http
                Host = rundbxml.lbdaq.cern.ch
                Port = 8080
              }
            }
            CERN-RAW
            {
              StorageBackend = Castor
              AccessProtocol.1
              {
                ProtocolName = RFIO
                Access = local
                Protocol = rfio
                Host = castorlhcb
                Port = 9002
                Path = /castor/cern.ch/grid
                SpaceToken = lhcbraw
              }
            }
          }
          Sites
          {
            DIRAC
            {
              DIRAC.ONLINE.ch
              {
                SE = CERN-RAW
                SE += OnlineRunDB
              }
            }
          }
          SiteLocalSEMapping
          {
            DIRAC.ONLINE.ch = CERN-RAW
            DIRAC.ONLINE.ch += OnlineRunDB
          }
        }


There is a special configuration for Request management

::

        Systems
        {
          RequestManagement
          {
            Production
            {
              Services
              {
                RequestManager
                {
                  LogLevel = INFO
                  HandlerPath = DIRAC/RequestManagementSystem/Service/RequestManagerHandler.py
                  Port = 9199
                  Protocol = dip
                  Backend = file
                  Path = /sw/dirac/requestDB
                  Authorization
                  {
                    Default = all
                  }
                }
              }
            }
          }
        }


###########################
Gateway DIRAC Installation
###########################

Just follow what is described in the documentation: http://dirac.readthedocs.io/en/latest/AdministratorGuide/InstallingDIRACService/index.html#install-primary-server

Use the install.cfg provied in the cfgTemplates

Then turn every component off in order to edit by hand the configuration files::

   runsvctrl d startup/*



************************************
Use the provided configuration files
************************************

Take the dirac.cfg and BOINC-conf.cfg in the template, and adapt them.
Remember that the dirac.cfg is used only by agents and services, and overwrites or adds little on top of the central CS.
The BOINC-Conf.cfg is what is served to the BOINC VMs.

The advantage of using these templates is that it is much easier and faster. But hopefuly, they will stay up to date...

***************************************
Generate the dirac.cfg (in progress...)
***************************************

Move content of the BOINCCert.cfg to dirac.cfg :
* all of Registry
* remove website part
* copy all the system definition

In dirac.cfg:
* replace boincInstance with the real instance you want to map to (boincInstance -> certification)
* replace DIRAC/Configuration/Servers with the real server
* LocalSite: name the gateway
* Registry: add MrBoincHost (after generating it)
* DIRAC/Security/UseServerCertificate = True
* In the URLs, ProxyManager -> BoincProxyManager


Edit the file:

* add the primary server URL
* change the setup to point to your real system and not local one



* run generateRunitEnv
* create the database by hand (ReqDB, proxyDB, sandboxstoreDB)
* Edit dirac.cfg

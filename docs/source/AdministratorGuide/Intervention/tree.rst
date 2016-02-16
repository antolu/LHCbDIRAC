=========================
Intervention on VOBOXes
=========================

Long intervention (as done the 22nd of February 2011)
-------------------------------------------------------
When a DIRAC setup with services, agent and databases running on separated boxes needs to be restarted due to a reboot of the servers, to
minimize the impact on running jobs a well define procedure should be followed.

You should put all the machine in maintenance mode.

::

  sms set maintenance 'power stop' other volhcbXX


Shutdown

1. stop all agents (**volhcb20**):

1.1 remove from startup directory all links to agents that are not to be restarted (that should be the normal condition for a production setup but needs to be checked).

1.2 stop the agents,
::

  runsvctrl d /opt/dirac/startup/*

1.3 move links from startup to a parallel directory to avoid their restart upon reboot of the machine. If the machine is not rebooted make sure that the processes are properly stopped.
::

  mkdir /opt/dirac/startup-save
  mv /opt/dirac/startup/* /opt/dirac/startup-save
  ps fax | grep run


2. stop all services (**volhcb19/volhcb18/volhcb15/volhcb17**)

2.1 remove from startup directory all links to services that are not to be restarted (that should be the normal condition for a production setup but needs to be checked).

2.2 stop the services,
::

  runsvctrl d /opt/dirac/startup/*


2.3 move links from startup to a parallel directory to avoid their restart upon reboot of the machine. If the machine is not rebooted make sure that the processes are properly stopped.
::

  mkdir /opt/dirac/startup-save
  mv /opt/dirac/startup/* /opt/dirac/startup-save
  ps fax | grep run


3. stop all databases (**volhcb21-26**)

::

  /opt/dirac/pro/mysql/share/mysql/mysql.server stop
  (for better performance after startup, move InnoDB "Journal" volhcb21-24:
  mv /opt/dirac/mysql/log /home/dirac/mysql-log
  ln -s /home/dirac/mysql-log /opt/dirac/mysql/log)


Startup.

the same procedure in the reverse order:

4. start all databases (**volhcb21-26**)

4.1 should occur automatically with the current configuration of the servers, will require a check to confirm it.

5. start of Framework Server (**volhcb18**)
::

  mv /opt/dirac/startup-save/* /opt/dirac/startup

check the logs for errors

6. start the rest of the services (**volhcb15,volhcb17,volhcb19**)
::

  mv /opt/dirac/startup-save/* /opt/dirac/startup

check the logs for errors and wait a bit before restarting the agents.

7. start the agents
::

  mv /opt/dirac/startup-save/* /opt/dirac/startup

check the logs for errors

As soon that all checks are OK, do not forget to put back the VOBOX in production.

::

  sms clear maintenance 'power stop' volhcbXX



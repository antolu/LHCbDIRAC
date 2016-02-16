The certification process
=========================

Certifying a release is a process. There are a number of steps to make to reach the point in which we can finally say that a release is at production level.
Within LHCbDirac, we are trying to streamline and automatize this process as much as possible. Even with that, some tests still require manual intervention.

We can split the process in a series of incremental tests, following what has been sketched in :ref:`develop`

Within the following sections we describe, step by step, all the actions needed. 


Unit test
---------

When a new release candidate is created from the devel branch, we first run pylint on the whole codebase, and all the unit tests. Jenkins automizes this for us.


Integration and Regression tests
---------------------------------

Run by Jenkins



System tests
------------

Even if it should not be considered strictly as a test, running all the agents and service within certification is an action to take.
Agents and services spits errors and exceptions. While the second are obviously bugs, the first are not to be considered bugs until an expert look. 
Nonetheless, we have created a tool to easily identify all new exceptions and errors:

::
	
	codeLocation=http://svn.cern.ch/guest/dirac/LHCbTestDirac/trunk/LHCbTestDirac/System/LogsParser/
	mkdir /tmp/logTest
	cd /tmp/logTest 
	wget -r -np -nH --cut-dirs=7 $codeLocation
	/bin/bash logParser.sh
	

In addition to the server side tests, at least 2 test files have been create that can be run on the client side. You can get them via:
::

   wget http://svnweb.cern.ch/world/wsvn/dirac/LHCbTestDirac/trunk/LHCbTestDirac/System/Client/client_test.csh
   wget http://svnweb.cern.ch/world/wsvn/dirac/LHCbTestDirac/trunk/LHCbTestDirac/System/GridTestSubmission/testUserJobs.py

and again simply run them (csh and python) 

For testing the SAM jobs, you can either run the SAMAgent (only for a short period), or better use the script
::
   
   dirac-lhcb-sam-submit
   

Testing the Bookkeeping: many of the tests that follow will test also that the bookkeeping works properly. 
Anyway, the first thing to do is to visit the bookkeeping `web page <https://volhcb30.cern.ch/DIRAC/LHCb-Certification/lhcb_prmgr/Data/BK/display>`_.

A second base test is simply to use the following command:
::
   dirac-bookkeeping-GUI


For testing that the RMS works, there is an ad-hoc test:
::

  wget http://github.com/DIRACGrid/DIRAC/blob/integration/DataManagementSystem/test/IntegrationFCT.py
  python IntegrationFCT.py lhcb_user CERN-USER RAL-USER CNAF-USER 
  python IntegrationFCT.py lhcb_prod CERN-FAILOVER RAL-FAILOVER CNAF-FALIOVER

Those commands will create and put to the Request Management System two new requests:

1. for lhcb_user group, which should be banned from using the FTS system 
2. for lhcb_prod or lhcb_prmgr group, which this should be executed using FTS

You could monitor their execution using `Request monitor` web page or by using CLI comamnd:

::

  dirac-rms-show-request test<userName>-<userGroup>

The execution itself will take a while, but at the end both requests statuses should be set to 'Done'.

Another test, again for the RMS, combined with FTS, is to simply use the following standard DIRAC scripts:

::

   dirac-dms-create-replication-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst

Which will actually schedule the replication of such file using FTS. This will print an ID that can be used for the script

::

   dirac-rms-show-request ID
   
That should show how the request goes (quickly) in status "Scheduled", and then "Done".  

The following script, instead, will remove the copy just created.  

::

   dirac-dms-create-removal-request CNAF_MC-DST /lhcb/certification/test/ALLSTREAMS.DST/00000751/0000/00000751_00000014_1.allstreams.dst
   
Again, monitoring is available as above.


For testing the replications and removals, use the following:
::

   dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --Test
   
That will just print out how many files can be replicated. If there is at least one file (for this particular query there should be 35), then you can start it with:
::

   dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=ReplicateDataset --NumberOfReplicas=2 --SecondarySEs Tier1-DST --Start 

   
You can monitor the advancement using: 
::

   dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST
   

Which should tell you the replica statistics, something like:
::

	[fstagni@lxplus0032 ~]$ dirac-dms-replica-stats --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST
	Executing BK query: {'Visible': 'Yes', 'ConfigName': 'validation', 'ConditionDescription': 'Beam3500GeV-2011-MagDown-Nu2-EmNoCuts', 'EventType': '12463412', 'FileType': 'ALLSTREAMS.DST', 'ConfigVersion': 'MC11a', 'ProcessingPass': '/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged', 'SimulationConditions': 'Beam3500GeV-2011-MagDown-Nu2-EmNoCuts'}

	34 files (0.0 TB) in directories:
	/lhcb/validation/MC11a/ALLSTREAMS.DST/00000654/0000 34 files
	34 files found with replicas

	Replica statistics:
	0 archives: 0 files
	1 archives: 25 files
	2 archives: 9 files
	0 replicas: 0 files
	1 replicas: 0 files
	2 replicas: 0 files
	3 replicas: 33 files
	4 replicas: 0 files
	5 replicas: 1 files
	
	SE statistics:
	    CERN-ARCHIVE: 15 files
	    CNAF-ARCHIVE: 5 files
	  GRIDKA-ARCHIVE: 11 files
	   IN2P3-ARCHIVE: 1 files
	     RAL-ARCHIVE: 8 files
	    SARA-ARCHIVE: 3 files
	   CERN_MC_M-DST: 34 files
	     CNAF_MC-DST: 4 files
	   CNAF_MC_M-DST: 8 files
	   GRIDKA_MC-DST: 1 files
	 GRIDKA_MC_M-DST: 3 files
	    IN2P3_MC-DST: 9 files
	  IN2P3_MC_M-DST: 6 files
	      PIC_MC-DST: 5 files
	    PIC_MC_M-DST: 4 files
	      RAL_MC-DST: 20 files
	    RAL_MC_M-DST: 6 files
	     SARA_MC-DST: 3 files
	   SARA_MC_M-DST: 1 files
	
	Sites statistics:
	     LCG.CERN.ch: 34 files
	     LCG.CNAF.it: 12 files
	   LCG.GRIDKA.de: 4 files
	    LCG.IN2P3.fr: 15 files
	      LCG.PIC.es: 9 files
	      LCG.RAL.uk: 26 files
	     LCG.SARA.nl: 4 files
	

Later, when you see that at least 2 replicas exist, you can issue
::

   dirac-dms-add-replication --BKQuery=/validation/MC11a/Beam3500GeV-2011-MagDown-Nu2-EmNoCuts/Sim05/Trig0x40760037Flagged/Reco12a/Stripping17Flagged/12463412/ALLSTREAMS.DST --Plugin=DeleteReplicas --NumberOfReplicas=1 --Start



Acceptance test steps
=====================

Installation of LHCbDirac
-------------------------

Login to a machine where LHCbDirac is already installed.
Set the LHCbDirac environment, get a proxy with admin rights and launch the sysadmin CLI

::

  SetupProject LHCbDirac
  lhcb-proxy-init -g diracAdmin
  dirac-admin-sysadmin-cli


Update the LHCbDirac version and restart all the services

::

  set host volhcbXX.cern.ch
  update LHCb-vArBpC
  restart *

Change the version of the pilot in the CS. Go to the web portal, login with your certificate and the role **diracAdmin**. Click on **Systems**, **Configuration** and **Manage Remote configuration**.

.. image:: images/CS_PilotVersion.png
   :height: 300pt


The version is in the section /Operations/lhcb/LHCb-Certification/Versions/PilotVersion. Clicks on the **PilotVersion** and on change option value.
Once you have changed the version number, click on **submit**. and do not forget to commit the change.

.. image:: images/CS_submit_change.png
   :height: 300pt


So you click on the left column on **Commit Configuration**

.. image:: images/CS_PilotVersion_OK.png
   :height: 300pt


Now you should restart the task queue director

::

   cd /opt/dirac/runit
   runsvctrl d WorkloadManagement/TaskQueueDirector
   runsvctrl u WorkloadManagement/TaskQueueDirector


Production test activity
------------------------

Open your browser and connect to the certification instance of the LHCbDirac web portal (http://lhcb-cert-dirac.cern.ch) select the setup **LHCb-Certification** and load your certificate in the portal. Check that that your role is **lhcb_user**.
Go to the tab **Production** and click on the **Requests** choice

.. image:: images/req1.png
   :height: 300pt

Click on the production which is defined label "template for certification" (nb = 28) and in the menu which appears select **Duplicate**

.. image:: images/req2.png
   :height: 300pt


You are ask if you want to **Clear the processing pass in the copy**. Select **No**. This will keep all the steps which are pre-defined.

.. image:: images/req3.png
   :height: 300pt

The new request is created and you get a number that will appear in the web page.

.. image:: images/req4.png
   :height: 300pt

Click on the new request that you just created the step below and select the **edit** option

.. image:: images/req5.png
   :height: 300pt

Then modify all the fields which needs a new value. Once you have finished, submit your request to the production team.

.. image:: images/req6.png
   :height: 300pt

You have just to approve it.

.. image:: images/req7.png
   :height: 400pt

Now you should change your role to become **lhcb_tech** and **lhcb_ppg** to validate the request. You click on the new request and in the menu you choose the option *sign*

.. image:: images/req8.png
   :height: 300pt

.. image:: images/req10.png
   :height: 300pt

You can sign or reject the request.

.. image:: images/req11.png
   :height: 300pt

Once the request has been accepted by lhcb_ppg and lhcb_tech, the status become **accepted**. Choose now the role **lhcb_pmgr** and click on the request. Then choose the option *edit*

.. image:: images/req12.png
   :height: 300pt

You give the correct Event Type and number of Events. Then you click on **Generate**
At this stage you are asked to choose which template should be used. In our case we will choose "MC_Simulation_run.py" and click on the **next** button.

.. image:: images/req13.png
   :height: 300pt

You get now the list of value that you could change before submitting the production. For the certification purpose you should change the value for "MC configuratioon name" to be **certification**, the "configuration version" should be **test**. Verify which plugin you want to use, the number of event that you want to process, the cputimelimit,... Once you have finished, click on the **generate** button.

.. image:: images/generate_prod.png
   :height: 300pt

After the generation of the production you will get in a new window the production ID and the number of jobs generated. If you want you can see and save the script which will generate this production by clicking on the **script preview** button.

.. image:: images/req16.png
   :height: 300pt

This is the window of the python script which could be used to generate again the production. To exit thi swindow click on **cancel**

.. image:: images/req17.png
   :height: 300pt

If you click on the request and you choose **production monitor** you will be re-direct to the production monitor.

.. image:: images/req18.png
   :height: 300pt

Production monitor with the fresh generated productions.

.. image:: images/req19.png
   :height: 300pt


dirac-bookkeeping-production-informations 830 -o /DIRAC/Setup=LHCb-Certification

::

	lxplus448] x86_64-slc5-gcc46-opt /afs/cern.ch/user/j/joel> dirac-bookkeeping-production-informations 830 -o /DIRAC/Setup=LHCb-Certification
	Production Info: 
	Configuration Name: LHCb
	Configuration Version: Collision11
	Event type: 91000000
	-----------------------
	StepName: merging MDF 
	ApplicationName    : mergeMDF
	ApplicationVersion : None
	OptionFiles        : None
	DDDB                : None
	CONDDB             : None
	ExtraPackages      :None
	-----------------------
	Number of Steps   1
	Total number of files: 2
	     LOG:1
	     RAW:1
	Number of events
	File Type           Number of events    Event Type          EventInputStat
	RAW                 30988               91000000            30988
	Path:  /LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Merging
	/LHCb/Collision11/Beam3500GeV-VeloClosed-MagDown/Real Data/Merging/91000000/RAW


You can then check the produced files: 

::

	nsls -l /castor/cern.ch/grid/lhcb/certification/test/ALLSTREAMS.DST/00000225/0000
	dirac-dms-lfn-replicas /lhcb/certification/test/ALLSTREAMS.DST/00000225/0000/00000225_00000001_1.allstreams.dst
	dirac-dms-add-replication --Production 259:268 --FileType RADIATIVE.DST --Plugin LHCbMCDSTBroadcastRandom --Request 30
	dirac-dms-add-replication --Production 239 --FileType ALLSTREAMS.DST --Plugin LHCbMCDSTBroadcastRandom --Request 29
	Transformation 273 created
	Name: Replication-ALLSTREAMS.DST-239-Request29 , Description: LHCbMCDSTBroadcastRandom of ALLSTREAMS.DST for productions 239
	BK Query: {'FileType': ['ALLSTREAMS.DST'], 'ProductionID': ['239'], 'Visibility': 'Yes'}
	3 files found for that query
	Plugin: LHCbMCDSTBroadcastRandom
	RequestID: 29
	[lxplus433] x86_64-slc5-gcc43-opt /afs/cern.ch/lhcb/software/DEV/LHCBDIRAC/LHCBDIRAC_v6r0-pre12> dirac-bookkeeping-production-informations 239Production Info::
	    Configuration Name: certification
	    Configuration Version: test
	    Event type: 12143001
	
	 StepName: MCMerging10
	    ApplicationName    : LHCb
	    ApplicationVersion : v31r7
	    OptionFiles        : $STDOPTS/PoolCopy.opts
	    DDB                : head-20101206
	    CONDDB             : sim-20101210-vc-md100
	    ExtraPackages      :None
	
	Number of Steps   4
	Total number of files: 8
	         LOG:4
	         ALLSTREAMS.DST:4
	Number of events
	File Type           Number of events    Event Type          EventInputStat
	ALLSTREAMS.DST      540                 12143001            540
	Path:  /certification/test/Beam3500GeV-VeloClosed-MagDown-Nu3/MC10Sim01-Trig0x002e002aFlagged/Reco08/Stripping12Flagged
	/certification/test/Beam3500GeV-VeloClosed-MagDown-Nu3/MC10Sim01-Trig0x002e002aFlagged/Reco08/Stripping12Flagged/12143001/ALLSTREAMS.DST
	 dirac-bookkeeping-production-files 239 ALLSTREAMS.DST
	FileName                                                                                             Size       GUID                                     Replica
	/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000044_1.allstreams.dst             14515993   165DD5A9-1D40-E011-AD80-003048F1E1E0     Yes
	/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000045_1.allstreams.dst             2971054    988731FC-1C40-E011-AFCD-90E6BA442F3B     Yes
	/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000074_1.allstreams.dst             202748580  E2BAF0A1-A340-E011-BF97-003048F1B834     Yes
	/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000076_1.allstreams.dst             2804277    F086C525-EB43-E011-96F9-001EC9D8B181     Yes
	
	[lxplus433] x86_64-slc5-gcc43-opt /afs/cern.ch/lhcb/software/DEV/LHCBDIRAC/LHCBDIRAC_v6r0-pre12> dirac-dms-lfn-replicas /lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000044_1.allstreams.dst
	{'Failed': {},
	 'Successful': {'/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000044_1.allstreams.dst': {'CERN_MC_M-DST': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000044_1.allstreams.dst'}}}
	[lxplus433] x86_64-slc5-gcc43-opt /afs/cern.ch/lhcb/software/DEV/LHCBDIRAC/LHCBDIRAC_v6r0-pre12> dirac-dms-lfn-replicas /lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000045_1.allstreams.dst
	{'Failed': {},
	 'Successful': {'/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000045_1.allstreams.dst': {'CNAF_MC_M-DST': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/t1d1/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000045_1.allstreams.dst'}}}
	[lxplus433] x86_64-slc5-gcc43-opt /afs/cern.ch/lhcb/software/DEV/LHCBDIRAC/LHCBDIRAC_v6r0-pre12> dirac-dms-lfn-replicas /lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000074_1.allstreams.dst
	{'Failed': {},
	 'Successful': {'/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000074_1.allstreams.dst': {'CERN_MC_M-DST': 'srm://srm-lhcb.cern.ch/castor/cern.ch/grid/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000074_1.allstreams.dst'}}}
	[lxplus433] x86_64-slc5-gcc43-opt /afs/cern.ch/lhcb/software/DEV/LHCBDIRAC/LHCBDIRAC_v6r0-pre12> dirac-dms-lfn-replicas /lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000076_1.allstreams.dst
	{'Failed': {},
	 'Successful': {'/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000076_1.allstreams.dst': {'CNAF_MC_M-DST': 'srm://storm-fe-lhcb.cr.cnaf.infn.it/t1d1/lhcb/certification/test/ALLSTREAMS.DST/00000239/0000/00000239_00000076_1.allstreams.dst'}}}


How to enable/disable FTS channel ? To check TFS transfer, look at the log for DataManagement/FTSSubmitAgent


Specific tests
--------------

Every release is somewhat special, and introduce new features that should be tested. 
It has to be noted that developers should always participate in the testing of very specific new developments, 
anyway the certification manager should look into if these tests have been done.

Within Jira, there is a special board, named `ready for integration <https://its.cern.ch/jira/secure/RapidBoard.jspa?rapidView=604&view=detail&>`_. 
that contain tasks marked as "Resolved", but not yet "Done". Dragging tasks from left to right will mark them as "Done".

So, the certification manager can decide to investigate directly, by submitting tests, if know, or ask the developer to confirm the task can be closed. 
 

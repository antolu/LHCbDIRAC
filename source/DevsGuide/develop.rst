Developing DIRAC and LHCbDIRAC
===============================

.. toctree::
   :maxdepth: 2

Developing the code is not just about editing. You also want to "run" something, usually for testing purposes. The DIRAC way of developing can be found `here <http://diracgrid.org/files/docs/DeveloperGuide/DevelopmentEnvironment/index.html>`_. The suggested way is in fact to install on your local machine a DIRAC version. The dirac-install script will try to fetch the pre-compiled externals. If the externals are not available for you distribution, so, unless you want to use a virtual machine, it might become problematic. You can also try to install the externals using your linux distribution packages, but you might get into some troubles. 

For example, on ubuntu you'll have to install packages like:

* python-mysqldb
* python-devel
* and many others

and then use `easy_install <https://pypi.python.org/pypi/setuptools>`_ (or `pip <https://pypi.python.org/pypi/pip>`_) to install pyGSI, that you can find in `github <https://github.com/acasajus/pyGSI>`_.

Also, remembers to update the $PYTHONPATH with the (LHCb)DIRAC directory. It is also good idea to add the scripts directory to your $PATH.


The Configuration Service (CS)
``````````````````````````````

At this point, the key becomes understanding how the DIRAC `Configuration Service (CS) <http://diracgrid.org/files/docs/AdministratorGuide/Configuration/ConfigurationStructure/index.html>`_ works. I'll explain here briefly. 

The CS is a layered structure: whenever you access a CS information (e.g. using a "gConfig" object, see later), DIRAC will first check into your local "dirac.cfg" file (it can be in your home as .dirac.cfg, or in etc/ directory, see the link above). 
If this will not be found, it will look for such info in the CS servers available. 

When you develop locally, you don't need to access any CS server: instead, you need to have total control. So, you need to work a bit on the local dirac.cfg file. 
There is not much else needed, just create your own etc/dirac.cfg. The example that follows might not be easy to understand at a first sight, but it will become easy soon. 
The syntax is extremely simple, yet verbose: simply, only brackets and equalities are used.       

I (Federico) can provide examples of it, if needed. In my local dirac.cfg file, among other things, I also specified:
::

	# Here we define the global setup of our DIRAC installation. 
	# Where there might be more than one setup, usually there's only one, as is in this example.
	# In this case, it happens to be called "MyLocalSetup".
	# Each setup is the "union" of "systems" setup. A "system" is, for example, "Transformation", or "WorkloadManagement".
	# Each system can have multiple "system setup", and in a "DIRAC setup" you can have systems from different "system setups"
	# In the example below, the DIRAC setup "MyLocalSetup" includes 8 systems, 6 of them having "MySetup" as setup, and 2 of them having "Development" setup.  
	DIRAC
	{
		Setups
		{
			MyLocalSetup
			{
				Framework = Development 
				Transformation = MySetup    
				ResourceStatus = MySetup 
				RequestManagement = MySetup
				Bookkeeping = Development
				WorkloadManagement = MySetup
				DataManagement = MySetup
				ProductionManagement = MySetup
			}
		}
	}
	
	#Here, we go to the definition of the single systems. Every system defined before has to be defined in what follows.
	Systems	
	{	
		#Unless you are a developer of the DIRAC framework, you don't want to change this part
		Framework
		{
			Development
			{
				URLs
				{
					# These are REAL URLs. Almost all of them point to volhcb12.cern.ch. 
					# Why? Because volhcb12.cern.ch is the "development" machine for the LHCb DIRAC installation.
					# Still, why doing that? Look at the names below: ProxyManager, SystemLogging...
					# All this stuff are framework services of DIRAC. So, for example, ProxyManager will keep your proxies,
					# and to test many (other) LHCb DIRAC components you need a proxy. 
					# So, you might still run these services locally, and add here the URLs, but why not using what is already running, for the developers??
					
					ProxyManager = dips://volhcb12.cern.ch:9152/Framework/ProxyManager
					SecurityLogging = dips://lhcbtest.pic.es:9141/Framework/SecurityLogging
					UserProfileManager = dips://lhcbtest.pic.es:9155/Framework/UserProfileManager
					Plotting = dips://volhcb12.cern.ch:9157/Framework/Plotting
					Notification = dips://volhcb12.cern.ch:9154/Framework/Notification
					BundleDelivery = dips://lhcb-sec-dirac.cern.ch:9158/Framework/BundleDelivery
					Monitoring = dips://volhcb12.cern.ch:9142/Framework/Monitoring
					SystemLogging = dips://volhcb12.cern.ch:9141/Framework/SystemLogging
					SystemLoggingReport = dips://volhcb12.cern.ch:9144/Framework/SystemLoggingReport
					SystemAdministrator = dips://volhcb12.cern.ch:9162/Framework/SystemAdministrator
					Gateway = dips://volhcb18.cern.ch:9199/Framework/Gateway
				}
			}
		}
		Bookkeeping
		{
			Development
			{
				# Unless you are developing changes to the bookkkeeping, you don't want to run a Bookkeeping on your local machine, do you? 
				# So, use the test Bookkeeping (again, this points to volhcb12.cern.ch), as you can see  
				URLs
				{
					bookkeepingManager = dips://volhcb12.cern.ch:9200/Bookkeeping/BookkeepingManager
				}
			}
		}
		
		# So, the 2 system above (Framework and Bookkeping) have the "Development" setup. This might be quite typical indeed.
		# What about the rest? Well, this is up to you. For example: are you developing for the TransformationSystem? You might want to add a "Transformation section".
		# And so on. 
		
		Transformation
		{
			MySetup
			{
				Agents
				{
					myNewAgent
					{
						option = value
						option2 = value1,value2

					}
				}
			}
		}
	}

So, if I want to test something, I run what I need to test locally. To do that, simply use the "dirac-agent" and "dirac-service" commands. For example, 

I can simply use 
::

	dirac-agent Transformation myNewAgent


Did you notice the connection between what's in the example of dirac.cfg, and what's above?


The CS, per-se, does not impose any fixed structure. What I mean is that the structure of the CS is in fact a convention. A very important convention, if I have to say.
Now, independently from your role, you can always look at what is in the production CS. You can simply point your browser `Here <https://lhcb-web-dirac.cern.ch/DIRAC/LHCb-Production/lhcb_user/systems/configuration/browseRemoteConfig>`_
It might be a bit instructive.

How to access the CS info? Try the following
::
	from DIRAC import gConfig
	gConfig.getValue('DIRAC/Setups/MyLocalSetup/Transformation')


Seen the connection? You might also explore the other functionalities of the gConfig object (which is in fact a singleton)


Within the CS, you can also specify a LocalSite part:
::

	LocalSite
	{
		Site = DIRAC.mySite.ch
		CPUScalingFactor = 0.0
		SharedArea = /home/myName/etc/LocalArea
		LocalArea =/home/myName/etc/LocalArea
		LocalSE = CERN-RAW
		LocalSE += CERN-DST
		LocalSE += CERN_M-DST
		LocalSE += CERN-USER
		LocalSE += CERN-FAILOVER
		LocalSE += CERN-RDST
		LocalSE += CERN_MC_M-DST
		LocalSE += MySE
	}

That is used when you want to submit jobs locally: the "site" where you stand will act like a worker node. This configuration also applies to AFS installations.



Code quality in LHCbDirac
=========================

Within this section we talk about code quality. There are few simple rules to follow, which involves testing your code, and writing simple and understandable code.


Testing
-------

First, few definitions. A complete suite of tests comprises functional and non-functional tests. Non-functional tests like scalability, maintainability, usability, performance, and security are out of the scope of this proposal. Also, we have at the moment no plans regarding static testings such as reviews, walkthroughs, or inspections: we'll just then consider dynamic tests.

The standard levels of a dynamic software testing process are:

1. Unit: testing the single modules in isolation.
2. Integration: test of group of modules. Regression tests falls here.
3. System: test of the whole system.
4. Acceptance: test the integration of the system with its dependencies, via a "user story".

In an ideal world, the execution of tests at all these levels would take part in a certification process. Within this section we identify the developers' responsibilities towards testing.

Tests at all the levels are executed in the most automated way possible. A reporting tool, including a code coverage system, is used. Tests are executed by the certification manager without interaction with the developers. The developers analyze the results of the tests as reported, and take actions if needed.


Unit tests
``````````

Developers are responsible to test the modules that are developed. We suggest that the developers of the single modules provide unit tests of the modules they develop. Python ``unittest`` module can be used. Even if the python standard library does not provide any *mocking* module, there are many out there free of use. DIRAC framework *could* be instrumented in this sense.

Modules already in use that are considered to be sufficiently excercised does not need to be unit-tested. But whenever a change is made into an existing module, the suggestion is to create a unit test for it. An important parameter for the unit tests is the code coverage: that higher it is, the better.

The advantage of using "standard" unittests instead of custom ones should be clear to everybody.

Unit tests could be part of a suite of certification tests, but in principal should be developed at the same time the single modules are created and modified, and checked constantly by the developers.


Integration tests
````````````````````

Package developers are in charge of creating integration tests that will test the interaction between different modules of the same package. The coverage of an integration test might vary, but should obviously be the larger possible. Python ``unittest`` and eventually mocking libraries could also be used,

Integration tests should be part of a suite of certification test suite. Obviously, nothing prevents the developers to run these tests even out of the certification process.
I actually advice to run such tests once you are developing for the chain client -> service -> database, which happens to happen quite frequently. Examples of such tests can be found in the TestDIRAC (git) and in the LHCbTestDIRAC (svn) repositories.


Regression tests
````````````````````

Usually similar to the integration tests, are a set of tests for checking the presence of a regression. They are defined either by the developer, or by the certification team.  


System tests
````````````

The certification team should be in charge of creating a system test. A system test is a black box test. It could be considered as the bulk of regression test for future releases. Anyway, its scope is to test the interaction between the different packages, and its code coverage, even if it may be large, might have leaks. Therefore, unit and integration tests are still necessary and should complement the use of one or more system test.


Acceptance tests
````````````````````

Acceptance tests are usually defined by the user of the systems. They should test some "user story". A "user story" could be interaction with the production request page, or test of the scripts.



Testing within LHCbDirac
``````````````````````````

Each developer should try to define as much unit tests as possible. Unit tests give security and, even if they are somewhat tedious to define, they are extremely useful when checking for code regression. 



Coding with rules
-----------------

The `pylint <http://www.pylint.org/>`_ reports dictate how you should write LHCbDirac code, and sometimes even discover real bugs. Please try to follow it as much as possible. See below for how Jenkins display it. 
  
`Jenkins <http://jenkins-ci.org/>`_ is a continuous integration server. LHCbDirac uses LHCb instance `here <https://lhcb-jenkins.cern.ch/jenkins/>`_. We use jenkins to run several things, not only pylint and not only the unit tests, but especially the integration and regression tests that can be found in the tests directory of the LHCbDIRAC repository. As of developer, you should pay attention to the pylint reports, and to run all the tests using `py.test <http://pytest.org>`_
    

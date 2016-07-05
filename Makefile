.PHONY: test scripts

#clean:
#	rm -rf *.out *.xml htmlcov

S=*System

test: 
	py.test $S --cov=$S

docs: 
	cd docs && make html && cd ..
	
OS = $(word 2,$(subst -, ,$(CMTCONFIG)))

MANIFEST = InstallArea/$(CMTCONFIG)/manifest.xml
XENV = InstallArea/$(CMTCONFIG)/LHCbDirac.xenv
DIST-TOOLS=dist-tools

PYTHON_VERSION=2.7.9.p1
PYTHON_VERSION_TWO=2.7
PYTHON_VERSION_TWODIGIT=27
LCGCMT_VERSION=84

ifeq ($(OS),slc6)
  DIRACPLAT=Linux_x86_64_glibc-2.12
endif
ifeq ($(OS),centos7)
  DIRACPLAT=Linux_x86_64_glibc-2.18
endif


#all: $(MANIFEST) requirements runit_tools $(XENV) $(XENV)c
all: gsi $(XENV) $(XENV)c scripts

scripts:
	$(DIST-TOOLS)/gen_scripts.py
gsi:
	mkdir -p InstallArea/$(CMTCONFIG)/lib
	$(DIST-TOOLS)/gen_GSI $(DIRACPLAT) $(PYTHON_VERSION_TWODIGIT) $(PYTHON_VERSION_TWO)
        
$(XENV) $(MANIFEST): Makefile
	python $(DIST-TOOLS)/gen_xenv.py -c $(CMTCONFIG) -f $(XENV) -m $(MANIFEST) -p $(PYTHON_VERSION_TWO) -d $(LHCBRELEASES)

$(XENV)c: $(XENV)
	xenv --xml $(XENV) true

clean:
	$(RM) $(XENV) $(XENV)c $(MANIFEST)
	
purge: clean
	$(RM) -r InstallArea/$(CMTCONFIG)
	$(RM) -r scripts

# fake targets to respect the interface of the Gaudi wrapper to CMake
configure:
install:
install/fast:
unsafe-install:
post-install:
OS = $(word 2,$(subst -, ,$(CMTCONFIG)))

MANIFEST = InstallArea/$(CMTCONFIG)/manifest.xml
XENV = InstallArea/$(CMTCONFIG)/LHCbDirac.xenv


REQUIREMENTS_TGTS = LHCbGridConfig/cmt/requirements LHCbGridSys/cmt/requirements


RUNIT_TOOLS = runit runit-init runsv runsvchdir runsvctrl runsvdir runsvstat chpst

PYTHON_VERSION=2.7.9.p1
PYTHON_VERSION_TWO=2.7
PYTHON_VERSION_TWODIGIT=27
LCGCMT_VERSION=83
PYGRAPHICS_VERSION=1.5_python$(PYTHON_VERSION_TWO)
GCC_VERSION=4.9.3
QT_VERSION=4.8.4
PYTOOLS_VERSION=1.9_python$(PYTHON_VERSION_TWO)
MYSQL_VERSION=5.5.27

LCG_VER=2015-01-09
ifeq ($(OS),slc6)
  DIRACPLAT=Linux_x86_64_glibc-2.12
endif
ifeq ($(OS),centos7)
  DIRACPLAT=Linux_x86_64_glibc-2.18
endif

DIRAC_BUNDLE=$(LHCBTAR)/DIRAC3/lcgBundles/DIRAC-lcg-$(LCG_VER)-$(DIRACPLAT)-python$(PYTHON_VERSION_TWODIGIT).tar.gz


#all: $(MANIFEST) requirements runit_tools $(XENV) $(XENV)c
all: gsi $(XENV) $(XENV)c

requirements: $(REQUIREMENTS_TGTS)

gsi:
	mkdir -p InstallArea/$(CMTCONFIG)/lib
	LHCbDiracConfig/gen_GSI $(DIRACPLAT) $(PYTHON_VERSION_TWODIGIT)
        
$(XENV): Makefile
	python LHCbDiracConfig/gen_xenv.py -f $@

$(XENV)c: $(XENV)
	xenv --xml $(XENV) true

runit_tools:
	mkdir -p InstallArea/$(CMTCONFIG)/bin
	tar -x -v -f $(DIRAC_BUNDLE) --xform='s#.*/\([^/]\+\)#InstallArea/$(CMTCONFIG)/bin/\1#' --show-transformed-names $(patsubst %,*/%,$(RUNIT_TOOLS))

clean:
	$(RM) $(MANIFEST) $(REQUIREMENTS_TGTS) $(patsubst %,InstallArea/$(CMTCONFIG)/bin/%,$(RUNIT_TOOLS))
purge: clean
	$(RM) -r InstallArea/$(CMTCONFIG)

# fake targets to respect the interface of the Gaudi wrapper to CMake
configure:
install:
install/fast:
unsafe-install:
post-install:
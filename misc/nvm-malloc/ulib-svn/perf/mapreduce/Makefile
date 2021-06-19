QUIET	?= @
INCPATH = ../../include
LIBPATH = ../../lib

CXXFLAGS ?=-O2 -W -Wall -I$(INCPATH)
#PROFILER  =-L../../../gperftools/lib -lprofiler 
#TCMALLOC  =-L../../../gperftools/lib -ltcmalloc
LDFLAGS   =-L$(LIBPATH) $(PROFILER) $(TCMALLOC) -lulib -lpthread -lrt

APP = $(addsuffix .test, $(basename $(wildcard *.cpp)))

%.test: %.cpp
	$(QUIET)echo -e "  GEN	"$@
	$(QUIET)$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

all: $(APP)

clean:
	$(QUIET)rm -rf $(APP) *~

.PHONY: all clean

CXX ?= g++
CXXFLAGS := -std=c++11 -Wall -Isrc -I../src
debug:   CXXFLAGS += -O0 -ggdb
release: CXXFLAGS += -O3
LDFLAGS := -L.. -lpthread -ltbb -lnvmmalloc

SRCDIR := src
BUILDDIR := build
BINARIES := bench_fastalloc bench_linkedlist bench_recovery bench_alloc_free bench_alloc_free_alloc
TARGETS := $(addprefix $(BUILDDIR)/, $(BINARIES))

release: $(TARGETS)

debug: $(TARGETS)

$(BUILDDIR)/bench_recovery: $(SRCDIR)/bench_recovery.cpp $(SRCDIR)/common.h $(SRCDIR)/common.cpp
	@mkdir -p $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -o $@ $(LDFLAGS) $< $(SRCDIR)/common.cpp

$(BUILDDIR)/%: $(SRCDIR)/%.cpp $(SRCDIR)/common.h $(SRCDIR)/common.cpp
	@mkdir -p $(BUILDDIR)
	$(CXX) $(CXXFLAGS) -DUSE_MALLOC -o $@ $(LDFLAGS) $< $(SRCDIR)/common.cpp
	$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -o $@_nvm $(LDFLAGS) $< $(SRCDIR)/common.cpp
	#$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -DHAS_CLFLUSHOPT -o $@_nvm_clflushopt $(LDFLAGS) $< $(SRCDIR)/common.cpp
	#$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -DHAS_CLWB -o $@_nvm_clwb $(LDFLAGS) $< $(SRCDIR)/common.cpp
	$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -o $@_nvm_noflush $(LDFLAGS)noflush $< $(SRCDIR)/common.cpp
	$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -o $@_nvm_nofence $(LDFLAGS)nofence $< $(SRCDIR)/common.cpp
	$(CXX) $(CXXFLAGS) -DUSE_NVM_MALLOC -o $@_nvm_none $(LDFLAGS)none $< $(SRCDIR)/common.cpp

clean:
	@rm -rf $(BUILDDIR)

.PHONY: clean

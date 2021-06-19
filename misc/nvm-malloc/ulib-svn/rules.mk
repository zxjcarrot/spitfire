QUIET		?= @
PREFIX		?= .
INCPATH		?= $(PREFIX)/include/ulib
LIBPATH		?= $(PREFIX)/lib
LIBNAME		?= undef

CFLAGS		?= -g3 -O2 -Wall -W -pipe -c -fPIC
CXXFLAGS	?= -g3 -O2 -Wall -W -pipe -c -fPIC
DEBUG		?=

OBJS		= \
		$(addsuffix .o, $(basename $(wildcard *.c))) \
		$(addsuffix .o, $(basename $(wildcard *.cpp)))

HEADERS		= $(wildcard *.h)

.c.o:
	$(QUIET)echo -e "  CC	"$<
	$(QUIET)$(CC) $(CFLAGS) -I$(INCPATH) $(DEBUG) $< -o $@

.cpp.o:
	$(QUIET)echo -e "  CXX	"$<
	$(QUIET)$(CXX) $(CXXFLAGS) -I$(INCPATH) $(DEBUG) $< -o $@

.PHONY: install_headers install_libs \
	uninstall_headers uninstall_libs \
	clean

clean: uninstall_headers uninstall_libs
	$(QUIET)rm -rf $(OBJS)
	$(QUIET)find . -name "*~" | xargs rm -rf

install_headers:
	$(QUIET)mkdir -p $(INCPATH)
	$(QUIET)cp $(HEADERS) $(INCPATH)/

install_libs: $(OBJS)
	$(QUIET)mkdir -p $(LIBPATH)
	$(QUIET)echo "  AR	"$(LIBPATH)/$(LIBNAME)
	$(QUIET)ar csr $(LIBPATH)/$(LIBNAME) $(OBJS)

uninstall_headers:
	$(QUIET)for file in $(HEADERS); do rm -rf $(INCPATH)/$$file; done

uninstall_libs:
	$(QUIET)rm -rf $(LIBPATH)/$(LIBNAME)

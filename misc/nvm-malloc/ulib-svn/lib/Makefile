QUIET	?= @

TARGET	= libulib.a

$(TARGET):
	$(QUIET)for lib in $(wildcard *.a); do $(AR) x $${lib}; done;
	$(QUIET)$(AR) csr $(TARGET) *.o
	$(QUIET)rm -rf *.o

debug: $(TARGET)

release: $(TARGET)
	$(QUIET)strip --strip-debug --strip-unneeded -w -K \* $(TARGET)

clean:
	$(QUIET)rm -rf *.a $(TARGET)

.PHONY: debug release clean

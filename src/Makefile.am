AM_CFLAGS = $(VARNISHAPI_CFLAGS)

# Modules

vmod_LTLIBRARIES = \
	libvmod_leastconn.la

libvmod_leastconn_la_LDFLAGS = $(VMOD_LDFLAGS)
libvmod_leastconn_la_SOURCES = vmod_leastconn.c
nodist_libvmod_leastconn_la_SOURCES = \
	vcc_leastconn_if.c \
	vcc_leastconn_if.h

@BUILD_VMOD_LEASTCONN@

# Test suite

AM_TESTS_ENVIRONMENT = \
	PATH="$(abs_builddir):$(VARNISH_TEST_PATH):$(PATH)" \
	LD_LIBRARY_PATH="$(VARNISH_LIBRARY_PATH)"
TEST_EXTENSIONS = .vtc
VTC_LOG_COMPILER = varnishtest -v
AM_VTC_LOG_FLAGS = \
	-p vcl_path="$(abs_top_srcdir)/vcl:$(VARNISHAPI_VCLDIR)" \
	-p vmod_path="$(abs_builddir)/.libs:$(vmoddir):$(VARNISHAPI_VMODDIR)"

TESTS = \
	vtc/vmod_leastconn.vtc

# Documentation

dist_doc_DATA = \
	vmod_leastconn.vcc \
	$(TESTS)

dist_man_MANS = \
	vmod_leastconn.3


.rst.1:
	$(AM_V_GEN) $(RST2MAN) $< $@

AC_PREREQ([2.68])
AC_INIT([vmod_leastconn], [0.1])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_HEADER([config.h])

AM_INIT_AUTOMAKE([1.12 -Wall -Werror foreign parallel-tests])
AM_SILENT_RULES([yes])
AM_PROG_AR

LT_PREREQ([2.2.6])
LT_INIT([dlopen disable-static])

AC_ARG_WITH([rst2man],
	AS_HELP_STRING(
		[--with-rst2man=PATH],
		[Location of rst2man (auto)]),
	[RST2MAN="$withval"],
	AC_CHECK_PROGS(RST2MAN, [rst2man rst2man.py], []))

VARNISH_PREREQ([6.0.0])
VARNISH_VMODS([leastconn])

AC_CONFIG_FILES([
	Makefile
	src/Makefile
	vmod_leastconn.spec
])

AC_OUTPUT

AS_ECHO("
	==== $PACKAGE_STRING ====

	varnish:      $VARNISH_VERSION
	prefix:       $prefix
	vmoddir:      $vmoddir
	vcldir:       $vcldir
	pkgvcldir:    $pkgvcldir

	compiler:     $CC
	cflags:       $CFLAGS
	ldflags:      $LDFLAGS
")

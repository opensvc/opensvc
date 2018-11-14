#!/bin/bash

PATH_LIB=$OSVC_PATH_COMP/com.opensvc
PREFIX=OSVC_COMP_FOO

typeset -i r=0

case $1 in
check)
	$OSVC_PYTHON $PATH_LIB/files.py ${PREFIX}_FILES check
	[ $? -eq 1 ] && r=1
	$OSVC_PYTHON $PATH_LIB/packages.py ${PREFIX}_PKG check
	[ $? -eq 1 ] && r=1
	exit $r
	;;
fix)
	$OSVC_PYTHON $PATH_LIB/files.py ${PREFIX}_FILES fix
	[ $? -eq 1 ] && exit 1
	$OSVC_PYTHON $PATH_LIB/packages.py ${PREFIX}_PKG fix
	[ $? -eq 1 ] && exit 1
	exit 0
	;;
fixable)
	exit 2
	;;
esac


SUMMARY="Cluster and configuration management agent"
DESCRIPTION="A cluster agent to deploy, start, stop, monitor and relocate applications \
described as services."

umask 022

function changelog {
	git log -n 100 --pretty=format:"* %cd %an <%ae>%n- %s" | \
	# strip tz
	sed -e "s/ [+\-][0-1][0-9][0-5][0-9] / /g" | \
	# strip time
	sed -e "s/ [0-2][0-9]:[0-5][0-9]:[0-5][0-9] / /g" | sed -e "s/%//" | \
	awk -v BRANCH=$BRANCH 'BEGIN{cmd="git describe --tags " BRANCH ; cmd | getline rev; split(rev, a, "-"); rev=a[2]}{if (rev<0){exit};if (/^\* /){print $0 " - " a[1] "-" rev} else {print; rev=rev-1}}'
}

function date_to_release {
	/opt/opensvc/bin/python -c "import datetime ; print(datetime.datetime.fromtimestamp($1).strftime('%y%m%d.%H%M'))"
}

cd $PATH_SCRIPT || {
    echo "Error : Can not change directory to $PATH_SCRIPT. Exiting."
    exit 1
}

#OFFSET=10000
HEAD=$(git log -1 --pretty=format:%at || exit 1)
VERSION=$(git describe --tags --abbrev=0)
RELEASE=$(git describe --tags|cut -d- -f2)

MASTER=0
git branch | grep "\* master" >/dev/null 2>&1 && MASTER=1

#if [ $MASTER -eq 1 ] ; then
#	let RELEASE=$RELEASE+$OFFSET
#fi

[ "$RELEASE" == "$VERSION" ] && RELEASE=0

OSVC=$PATH_SCRIPT/../..
CHROOT_BN=opensvc-$VERSION-$RELEASE
CHROOT=$OSVC/tmp/BUILDROOT/$CHROOT_BN

if [ "$(uname)" == "SunOS" -a -x /usr/xpg4/bin/id ] ; then
	ID_BIN="/usr/xpg4/bin/id"
else
	ID_BIN="id"
fi

if [ "$($ID_BIN -u)" == "0" ] ; then
	SUDO=""
else
	SUDO="sudo"
fi

function deploy_cluster_manager {


	# OSVC_CLUMGR_REPO can be set to use a custom repository
	# OSVC_CURL_OPTS can be set to custom download parameters
        CLUMGR_DIR="$CHROOT/opt/opensvc/usr/share/html"
        API_VERSION=$(grep ^API_VERSION $CHROOT/opt/opensvc/opensvc/daemon/shared.py | awk '{print $3}')
	CLUMGR_REPO=${OSVC_CLUMGR_REPO:-https://repo.opensvc.com/cluster-manager}
        CLUMGR_URL="${CLUMGR_REPO}/${API_VERSION}/current"
        CLUMGR_BUNDLE="$CHROOT/opt/opensvc/bundle.tar.gz"

	echo $CLUMGR_REPO | grep -qw "^https"
	ret=$?
	if [ $ret -eq 0 ]; then
	    CURL_OPTS=${OSVC_CURL_OPTS:--k -s}
	else
	    CURL_OPTS=${OSVC_CURL_OPTS:--s}
	fi
        
        [[ ! -d $CLUMGR_DIR ]] && mkdir -p $CLUMGR_DIR

	# need curl to be available
        if ! [ -x "$(command -v curl)" ]; then
          echo 'Warning: curl is not installed. skipping cluster manager installation' >&2
          return
        fi

	echo "Downloading cluster manager bundle from ${CLUMGR_URL}"
        curl $CURL_OPTS -o $CLUMGR_BUNDLE $CLUMGR_URL || {
           echo 'Warning: could not download cluster manager bundle. skipping cluster manager installation' >&2
           return
        }

        echo "Extracting cluster manager bundle to $CLUMGR_DIR"
        tar xzf $CLUMGR_BUNDLE -C $CLUMGR_DIR 2>/dev/null || {
           echo 'Warning: could not extract cluster manager bundle. skipping cluster manager installation' >&2
	   return
        }
}

function prepare_chroot {
        # cleanup
        $SUDO rm -rf $OSVC/tmp/BUILDROOT/*
	#
	# prepare skeleton (data.tar.gz)
	#
	echo $CHROOT | grep noarch >/dev/null 2>&1 && $SUDO rm -rf $CHROOT

	mkdir -p $CHROOT/opt/opensvc

	# install tracked files
	cd $OSVC || return 1
	git config tar.umask 0022
	git archive --format=tar HEAD | ( cd $CHROOT/opt/opensvc && tar xf - )

	# install version.py
           echo "version = \"$VERSION-$RELEASE\"" | tee $CHROOT/opt/opensvc/opensvc/utilities/version/version.py || return 1

	# make and compress docs
	$CHROOT/opt/opensvc/bin/pkg/make_doc
	cd $CHROOT/opt/opensvc/usr/share/doc
	for f in $(echo template*conf)
	do
		echo "gzip -9 -n $f"
		gzip -9 -n $f
	done
	cd $CHROOT/opt/opensvc/usr/share/man/man1
	for f in $(echo *.1)
	do
		echo "gzip -9 -n $f"
		gzip -9 -n $f
	done

        deploy_cluster_manager

	# purge unwanted files
	cd /tmp
	rm -f $CHROOT/opt/opensvc/*.tar.gz >> /dev/null 2>&1
	rm -rf $CHROOT/opt/opensvc/bin/pkg
	rm -rf $CHROOT/opt/opensvc/opensvc/tests
	rm -f $CHROOT/opt/opensvc/bin/postinstall.cmd

	DOCDIR=$CHROOT/usr/share/doc/opensvc
	rpm --test >/dev/null 2>&1 && {
		DOCDIR=$CHROOT/$(rpm --eval '%{_defaultdocdir}')/opensvc
	}

	# move files to LSB locations
	mkdir -p $CHROOT/usr/bin
	mkdir -p $CHROOT/etc/opensvc
	mkdir -p $CHROOT/var/log/opensvc
	mkdir -p $CHROOT/var/lib/opensvc/cache
	mkdir -p $CHROOT/usr/share/opensvc
	mkdir -p $DOCDIR
	mkdir -p $CHROOT/usr/share/man/man1
	mkdir -p $CHROOT/etc/bash_completion.d
	mv $CHROOT/opt/opensvc/bin $CHROOT/usr/share/opensvc/
	mv $CHROOT/opt/opensvc/opensvc $CHROOT/usr/share/opensvc/
	mv $CHROOT/opt/opensvc/var/compliance $CHROOT/var/lib/opensvc/
	mv $CHROOT/opt/opensvc/usr/share/doc/* $DOCDIR/
	mv $CHROOT/opt/opensvc/usr/share/man/man1/* $CHROOT/usr/share/man/man1/
	mv $CHROOT/opt/opensvc/usr/share/html $CHROOT/usr/share/opensvc/
	mv $CHROOT/opt/opensvc/usr/share/bash_completion.d/opensvc.sh $CHROOT/etc/bash_completion.d/
	ln -sf ../share/opensvc/bin/om $CHROOT/usr/bin/om
	ln -sf ../share/opensvc/bin/opensvc $CHROOT/usr/bin/svcmgr
	ln -sf ../share/opensvc/bin/opensvc $CHROOT/usr/bin/nodemgr
	ln -sf ../share/opensvc/bin/opensvc $CHROOT/usr/bin/svcmon

	# purge unwanted files
	rm -rf $CHROOT/opt/opensvc
        rmdir $CHROOT/opt

	cd $OSVC
	$SUDO chown -Rh 0:0 $CHROOT/* || return 1
	$SUDO find $CHROOT -type d -exec $SUDO chmod 755 {} \; || return 1
	$SUDO find $CHROOT/etc/bash_completion.d -type f -exec $SUDO chmod 644 {} \; || return 1
	$SUDO find $CHROOT/usr/share/man -type f -exec $SUDO chmod 644 {} \; || return 1
	$SUDO find $CHROOT/usr/share/doc -type f -exec $SUDO chmod 644 {} \; || return 1

	return 0
}


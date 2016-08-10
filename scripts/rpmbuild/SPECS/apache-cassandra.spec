%define __jar_repack %{nil}

%global username cassandra

%define relname %{name}-%{version}

Name:           apache-cassandra
Version:        %{_instagram_version}
Release:        1%{?packager}
Summary:        Cassandra is a highly scalable, eventually consistent, distributed, structured key-value store.

Group:          Development/Libraries
License:        Apache Software License
URL:            http://cassandra.apache.org/
Source0:	http://apache.mirror3.ip-only.net/cassandra/%{version}/apache-cassandra-%{version}-bin.tar.gz
Source1:	cassandra.sh
Patch0:		patch_redhat
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

# BuildRequires: java-devel
BuildRequires: jpackage-utils

Conflicts:     cassandra

Requires:      java >= 1.7.0
#Requires:      jna  >= 4.0.0
Requires:      jpackage-utils
Requires(pre): user(cassandra)
Requires(pre): group(cassandra)
Requires(pre): shadow-utils
Provides:      user(cassandra)
Provides:      group(cassandra)

BuildArch:      noarch

%description
Cassandra brings together the distributed systems technologies from Dynamo
and the data model from Google's BigTable. Like Dynamo, Cassandra is
eventually consistent. Like BigTable, Cassandra provides a ColumnFamily-based
data model richer than typical key/value systems.

For more information see http://cassandra.apache.org/

%prep
%setup -q -n %{relname}
%patch0 -p1

%build

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}%{_sysconfdir}/%{username}/
mkdir -p %{buildroot}/usr/share/%{username}
mkdir -p %{buildroot}/usr/share/%{username}/lib
mkdir -p %{buildroot}/usr/share/%{username}/pylib
mkdir -p %{buildroot}/usr/share/%{username}/pylib/cqlshlib/
mkdir -p %{buildroot}/usr/share/%{username}/default.conf
mkdir -p %{buildroot}%{_sysconfdir}/%{username}/default.conf
mkdir -p %{buildroot}%{_sysconfdir}/rc.d/init.d/
mkdir -p %{buildroot}%{_sysconfdir}/security/limits.d/
mkdir -p %{buildroot}%{_sysconfdir}/default/
mkdir -p %{buildroot}/usr/sbin
mkdir -p %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_sysconfdir}/profile.d/
cp -rp conf/* %{buildroot}%{_sysconfdir}/%{username}/default.conf
cp -rp conf/* %{buildroot}/usr/share/%{username}/default.conf
cp -p redhat/%{username} %{buildroot}%{_sysconfdir}/rc.d/init.d/
cp -p redhat/%{username}.conf %{buildroot}%{_sysconfdir}/security/limits.d/
cp -p redhat/default %{buildroot}%{_sysconfdir}/default/%{username}
cp -p lib/*.jar %{buildroot}/usr/share/%{username}/lib
cp -p tools/lib/*.jar %{buildroot}/usr/share/%{username}/lib
cp -p pylib/*.py %{buildroot}/usr/share/%{username}/pylib
cp -p pylib/cqlshlib/*.py %{buildroot}/usr/share/%{username}/pylib/cqlshlib
cp -p %{SOURCE1} %{buildroot}%{_sysconfdir}/profile.d/
mv redhat/cassandra.in.sh %{buildroot}/usr/share/%{username}
rm bin/cassandra.in.sh
mv bin/cassandra %{buildroot}/usr/sbin
rm bin/*.bat
cp -p bin/* %{buildroot}%{_bindir}
mkdir -p %{buildroot}%{_localstatedir}/lib/%{username}/commitlog
mkdir -p %{buildroot}%{_localstatedir}/lib/%{username}/data
mkdir -p %{buildroot}%{_localstatedir}/lib/%{username}/saved_caches
mkdir -p %{buildroot}%{_localstatedir}/run/%{username}
mkdir -p %{buildroot}%{_localstatedir}/log/%{username}

%clean
%{__rm} -rf %{buildroot}

%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /usr/share/%{username} -g %{username} -M -r %{username}
exit 0

%preun
# only delete user on removal, not upgrade
if [ "$1" = "0" ]; then
    userdel %{username}
fi

%files
%defattr(-,root,root,0755)
%doc CHANGES.txt LICENSE.txt NEWS.txt NOTICE.txt
%attr(755,root,root) %{_bindir}/*
%attr(755,root,root) %{_sbindir}/cassandra
%attr(755,root,root) %{_sysconfdir}/rc.d/init.d/%{username}
%attr(755,root,root) %{_sysconfdir}/default/%{username}
%attr(755,root,root) %{_sysconfdir}/security/limits.d/%{username}.conf
%attr(755,root,root) %{_sysconfdir}/profile.d/*
%attr(755,%{username},%{username}) /usr/share/%{username}*
%attr(755,%{username},%{username}) %config(noreplace) /%{_sysconfdir}/%{username}
%attr(755,%{username},%{username}) %config(noreplace) %{_localstatedir}/lib/%{username}/*
%attr(755,%{username},%{username}) %{_localstatedir}/log/%{username}*
%attr(755,%{username},%{username}) %{_localstatedir}/run/%{username}*

%post
alternatives --install %{_sysconfdir}/%{username}/conf %{username} %{_sysconfdir}/%{username}/default.conf/ 0
exit 0

%postun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    alternatives --remove %{username} %{_sysconfdir}/%{username}/default.conf/
fi
exit 0

%changelog
* Sun Feb 10 2013 Karl Böhlmark <karl.bohlmark@gmail.com> - 1.2.1-1
- Updated version, switched to binary release.
* Mon Aug 20 2012  William Wichgers <wwichgers@rim.com> - 1.1.4-1
- Updated version
* Tue Aug 6 2012  William Wichgers <wwichgers@rim.com> - 1.1.3-1
- Updated version
* Tue Jul 6 2012  William Wichgers <wwichgers@rim.com> - 1.1.2-1
- Updated version
* Tue Jun 5 2012  William Wichgers <wwichgers@rim.com> - 1.1.1-1
- Updated version
* Fri May 22 2012 William Wichgers <wwichgers@rim.com> - 1.1.0-1
- Updated version
* Wed Nov 30 2011 Andrew Grimberg <andrew@gist.com> - 1.0.3-1
- Updated version
* Wed Sep 21 2011 Andrew Grimberg <andrew@gist.com> - 0.8.6-1
- Updated version
* Mon Mar 07 2011 Andrew Grimberg <andrew@gist.com> - 0.7.3-1
- Updated version
- Fixed relname to not include release version number
- Add in a profile.d export for CASSANDRA_HOME & CASSANDRA_CONF
* Tue Aug 03 2010 Nick Bailey <nicholas.bailey@rackpace.com> - 0.7.0-1
- Updated to make configuration easier and changed package name.
* Mon Jul 05 2010 Peter Halliday <phalliday@excelsiorsystems.net> - 0.6.3-1
- Initial package

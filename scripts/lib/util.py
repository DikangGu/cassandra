# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree.

import logging
import os.path
import shutil
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

try:
    socket.gethostbyname('fwdproxy')
    USE_PROXY = True
except:
    logging.debug('Can\'t access fwdproxy, probably running locally.')
    USE_PROXY = False

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))


def get_base_version():
    """Get base.version from build.xml"""
    build_xml = os.path.join(PROJECT_ROOT, 'build.xml')
    root = ET.parse(build_xml).getroot()
    for _property in root.findall('property'):
        if _property.get('name') == 'base.version':
            return _property.get('value')
    raise KeyError('Can\'t find property with name `base.version` in build.xml.')


def get_head_revision():
    p = subprocess.Popen('git rev-parse --short HEAD',
                         shell=True, cwd=os.path.join(PROJECT_ROOT),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(err)
    return output.strip()


def get_instagram_version():
    base_version = get_base_version()
    date = time.strftime('%Y%m%d')
    head_revision = get_head_revision()
    return '%s+git%s.%s' % (base_version, date, head_revision)


def rpmbuild(artifact_file, version):
    rpmbuild_dir = os.path.join(PROJECT_ROOT, 'scripts/rpmbuild')
    shutil.copyfile(artifact_file,
                    os.path.join(rpmbuild_dir, 'SOURCES', os.path.basename(artifact_file)))
    subprocess.check_call(['rpmbuild',
                           '-ba',
                           '--define', '_topdir %s' % rpmbuild_dir,
                           '--define', '_instagram_version %s' % version,
                           'SPECS/apache-cassandra.spec'],
                           cwd=rpmbuild_dir)
    return os.path.join(rpmbuild_dir, 'RPMS/noarch/apache-cassandra-%s-1.noarch.rpm' % version)

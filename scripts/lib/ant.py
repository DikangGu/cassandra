# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree.

import logging
import os
import os.path
import subprocess
import sys

from util import (
    PROJECT_ROOT,
    USE_PROXY,
)

BUILD_FILE = os.path.join(PROJECT_ROOT, 'ig-build.xml')

def run_ant_target(target_name, options=[]):
    env = os.environ.copy()
    if USE_PROXY:
        env['ANT_OPTS'] = ('-Dhttp.proxyHost=fwdproxy -Dhttp.proxyPort=8080 -Djava.net.preferIPv6Addresses=true ' +
                           '-Dhttps.proxyHost=fwdproxy -Dhttps.proxyPort=8080')
    try:
        # Redirect subprocess's stdout to stderr, so that we could write formatted result to stdout.
        subprocess.check_call(['ant', '-f', BUILD_FILE, target_name] + options,
                              cwd=PROJECT_ROOT, env=env, stdout=sys.stderr)
    except subprocess.CalledProcessError as e:
        return False
    return True

def build():
    return run_ant_target('build-src-unit')


def clean():
    return run_ant_target('clean')


def list_unit_tests():
    """List all unit tests by class name.
    By listing all the *Test.java file under $PROJECT_ROOT/test/unit folder."""

    p = subprocess.Popen("find ./ -name '*Test.java' | cut -c 3- | sed -e 's/\//./g; s/\.java//g'",
                          shell=True, cwd=os.path.join(PROJECT_ROOT, './test/unit/'),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(err)

    return filter(lambda test: len(test) > 0, map(lambda test: test.strip(), output.split('\n')))


def run_test(test_name):
    return run_ant_target('test-single', ['-Dtest.name=%s'%test_name])


def run_all_test():
    return run_ant_target('test')

# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree.

import logging
import os.path
import socket

try:
    socket.gethostbyname('fwdproxy')
    USE_PROXY = True
except:
    logging.debug('Can\'t access fwdproxy, probably running locally.')
    USE_PROXY = False

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

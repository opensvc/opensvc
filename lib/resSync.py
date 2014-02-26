#
# Copyright (c) 2011 Christophe Varoqui <christophe.varoqui@opensvc.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
import os
import logging

import rcExceptions as ex
import resources as Res
import datetime
import time
from rcGlobalEnv import rcEnv

class Sync(Res.Resource):
    def __init__(self,
                 rid=None,
                 sync_max_delay=None,
                 sync_interval=None,
                 sync_period=None,
                 sync_days=None,
                 optional=False,
                 disabled=False,
                 tags=set([]),
                 type=type,
                 subset=None):
        if sync_max_delay is None:
            self.sync_max_delay = 1500
        else:
            self.sync_max_delay = sync_max_delay

        if sync_interval is None:
            self.sync_interval = 121
        else:
            self.sync_interval = sync_interval

        if sync_period is None:
            self.sync_period = ["03:59", "05:59"]
        else:
            self.sync_period = sync_period

        if sync_days is None:
            self.sync_days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        else:
            self.sync_days = sync_days

        Res.Resource.__init__(self,
                              rid=rid,
                              type=type,
                              optional=optional,
                              disabled=disabled,
                              tags=tags,
                              subset=subset)

    def check_timestamp(self, ts, comp='more', delay=10):
        """ Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval
        """
        if delay == 0:
            raise
        limit = ts + datetime.timedelta(minutes=delay)
        if comp == "more" and datetime.datetime.now() < limit:
            return False
        elif comp == "less" and datetime.datetime.now() < limit:
            return False
        else:
            return True
        return True

    def in_period(self, period=None):
        if period is None:
            period = self.sync_period
        if len(period) == 0:
            return True
        if isinstance(period[0], list):
            r = False
            for p in period:
                 r |= self.in_period(p)
            return r
        elif (not isinstance(period[0], unicode) and \
              not isinstance(period[0], str)) or \
             len(period) != 2 or \
             (not isinstance(period[1], unicode) and \
              not isinstance(period[1], str)):
            self.log.error("malformed period: %s"%str(period))
            return False
        start_s, end_s = period
        try:
            start_t = time.strptime(start_s, "%H:%M")
            end_t = time.strptime(end_s, "%H:%M")
        except ValueError:
            self.log.error("malformed period: %s"%str(period))
            return False
        start = start_t.tm_hour * 60 + start_t.tm_min
        end = end_t.tm_hour * 60 + end_t.tm_min
        try:
            start_t = time.strptime(start_s, "%H:%M")
            end_t = time.strptime(end_s, "%H:%M")
            start = start_t.tm_hour * 60 + start_t.tm_min
            end = end_t.tm_hour * 60 + end_t.tm_min
        except:
            self.log.error("malformed time string: %s"%str(period))
            return False
        now = datetime.datetime.now()
        now_m = now.hour * 60 + now.minute
        if start <= end:
            if now_m >= start and now_m <= end:
                return True
        elif start > end:
            """
                  XXXXXXXXXXXXXXXXX
                  23h     0h      1h
            """
            if (now_m >= start and now_m <= 1440) or \
               (now_m >= 0 and now_m <= end):
                return True
        return False

    def in_days(self):
        now = datetime.datetime.now()
        today = now.strftime('%A').lower()
        if today in map(lambda x: x.lower(), self.sync_days):
            return True
        return False

    def skip_sync(self, ts):
        if self.svc.force:
            return False
        if self.sync_interval == 0:
            self.log.info('skip sync: disabled by sync_interval = 0')
            return True
        if not self.in_days():
            self.log.info('skip sync: not in allowed days (%s)'%str(self.sync_days))
            return True
        if not self.in_period():
            self.log.info('skip sync: not in allowed period (%s)'%str(self.sync_period))
            return True
        if ts is None:
            self.log.info("don't skip sync: no timestamp")
        elif not self.check_timestamp(ts, comp="less", delay=self.sync_interval):
            self.log.info('skip sync: too soon (%d)'%self.sync_interval)
            return True
        return False

    def alert_sync(self, ts):
        if ts is None:
            return True
        if not self.check_timestamp(ts, comp="less", delay=self.sync_max_delay):
            return False
        return True

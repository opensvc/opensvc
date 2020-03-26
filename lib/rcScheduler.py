"""
This module defines the Scheduler class inherited by Svc and Node.
"""
from __future__ import print_function
import sys
import os
import datetime
import json
import time
import random
import logging

import exceptions as ex
from rcGlobalEnv import rcEnv
from storage import Storage
from utilities.render.color import formatter, color
from utilities.string import is_string
from converters import convert_duration

SCHED_FMT = "%s: %s"
CALENDAR_NAMES = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

class SchedNotAllowed(Exception):
    """
    The exception signaling the task can not run due to scheduling
    constaints.
    """
    pass

class SchedNoDefault(Exception):
    """
    The exception raised to signal a task has no default schedule
    defined.
    """
    pass

class SchedSyntaxError(Exception):
    """
    The exception raised to signal the defined schedule has syntax
    errors.
    """
    pass

class SchedExcluded(Exception):
    """
    The exception raised to signal a negative constraint violation.
    """
    pass

class SchedOpts(object):
    """
    The class storing a task schedule options.
    """
    def __init__(self, section,
                 fname=None,
                 schedule_option="push_schedule",
                 req_collector=False):
        self.section = section
        self.fname = fname
        self.req_collector = req_collector
        if self.fname is None:
            self.fname = "last_"+section+"_push"
        self.schedule_option = schedule_option

def sched_action(func):
    """
    A decorator in charge of updating the scheduler tasks and subtasks
    timestamps.
    """
    def _func(self, action, options=None):
        if options is None:
            options = Storage()
        if action in self.sched.scheduler_actions:
            self.sched.action_timestamps(action, options.rid)
        try:
            ret = func(self, action, options)
        except ex.AbortAction:
            # finer-grained locking can raise that to cancel the task
            return 0
        if ret == 0 and action in self.sched.scheduler_actions:
            self.sched.action_timestamps(action, options.rid, success=True)
        return ret
    return _func

class Scheduler(object):
    """
    The scheduler class.

    The node and each service inherit an independent scheduler through
    this class.
    """
    def __init__(self, config_defaults=None, node=None, options=None,
                 scheduler_actions=None, log=None, svc=None):
        self.config_defaults = config_defaults

        if scheduler_actions is None:
            self.scheduler_actions = {}
        else:
            self.scheduler_actions = scheduler_actions

        if options is None:
            self.options = Storage()
        else:
            self.options = options

        self.svc = svc
        self.node = node
        if svc:
            self.obj = svc
            self.log = svc.log
            if node is None:
                self.node = svc.node
        else:
            self.obj = node
            self.log = node.log
            if node is None:
                self.node = node

    def get_next_schedule(self, action, _max=14400):
        """
        Iterate future dates in search for the next date validating
        <action> scheduling constraints.
        """
        now = datetime.datetime.now()
        cron = self.options.cron
        self.options.cron = True
        for idx in range(_max):
            future_dt = now + datetime.timedelta(minutes=idx*10)
            data = self.skip_action(action, now=future_dt)
            if isinstance(data, dict):
                if len(data["keep"]) > 0:
                    return {"next_sched": future_dt, "minutes": _max}
            elif not data:
                self.options.cron = cron
                return {"next_sched": future_dt, "minutes": _max}
        self.options.cron = cron
        return {"next_sched": None, "minutes": None}

    @staticmethod
    def _need_action_interval(last, delay=10, now=None):
        """
        Return False if timestamp is fresher than now-interval
        Return True otherwize.
        Zero is a infinite interval.
        """
        if delay == 0:
            return False
        if last is None:
            return True
        if now is None:
            now = datetime.datetime.now()
        ds = delay * 60
        limit = last + datetime.timedelta(seconds=ds)
        return now >= limit

    def _timestamp(self, timestamp_f, last=None):
        """
        Update the timestamp file <timestamp_f>.
        If <timestamp_f> if is not a fullpath, consider it parented to
        <pathvar>.
        Create missing parent directories if needed.
        """
        if last is None:
            last = datetime.datetime.now()
        if not timestamp_f.startswith(os.sep):
            timestamp_f = self.get_timestamp_f(timestamp_f)
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d, 0o755)
        with open(timestamp_f, 'w') as ofile:
            buff = last.strftime("%Y-%m-%d %H:%M:%S.%f")
            ofile.write(buff+os.linesep)
        return True

    def _skip_action_interval(self, last, interval, now=None):
        """
        Return the negation of _need_action_interval()
        """
        return not self._need_action_interval(last, interval, now=now)

    def _timerange_delay(self, timerange, now=None):
        """
        Return a delay in seconds, compatible with the timerange.

        The daemon scheduler thread will honor this delay,
        executing the task only when expired.

        This algo is meant to level collector's load which peaks
        when tasks trigger at the same minute on every nodes.
        """
        if not timerange.get("probabilistic", False):
            return 0

        try:
            begin = self._time_to_minutes(timerange["begin"])
            end = self._time_to_minutes(timerange["end"])
            now = self._time_to_minutes(now)
        except:
            raise SchedNotAllowed("time conversion error delay eval")

        # day change in the timerange
        if begin > end:
            end += 1440
        if now < begin:
            now += 1440

        length = end - begin
        remaining = end - now - 1

        if remaining < 10:
            # no need to delay for tasks with a short remaining valid time
            return 0

        if timerange["interval"] < length:
            # don't delay if interval < period length, because the user
            # expects the action to run multiple times in the period. And
            # '@<n>' interval-only schedule are already different across
            # nodes due to daemons not starting at the same moment.
            return 0

        rnd = random.random()

        return int(remaining*60*rnd)

    @staticmethod
    def _time_to_minutes(dt_spec):
        """
        Convert a datetime or a %H:%M formatted string to minutes.
        """
        if isinstance(dt_spec, datetime.datetime):
            dtm = dt_spec
            dt_spec = dtm.hour * 60 + dtm.minute
        else:
            try:
                dtm = time.strptime(dt_spec, "%H:%M")
            except:
                raise Exception("malformed time string: %s"%str(dt_spec))
            dt_spec = dtm.tm_hour * 60 + dtm.tm_min
        return dt_spec

    def _in_timeranges(self, schedule, fname=None, now=None, last=None):
        """
        Validate the timerange constraints of a schedule.
        Iterates multiple allowed timeranges.

        Return a delay the caller should wait before executing the task,
        with garanty the delay doesn't reach outside the valid timerange:

        * 0 => immediate execution
        * n => seconds to wait

        Raises SchedNotAllowed if the validation fails the timerange
        constraints.
        """
        if len(schedule["timeranges"]) == 0:
            raise SchedNotAllowed("no timeranges")
        errors = []
        delay = 0
        for timerange in schedule["timeranges"]:
            try:
                self.in_timerange(timerange, now=now)
                self.in_timerange_interval(timerange, fname=fname, now=now, last=last)
                if fname is not None:
                    # fname as None indicates we run in test mode
                    delay = self._timerange_delay(timerange, now=now)
                return delay
            except SchedNotAllowed as exc:
                errors.append(str(exc))
        raise SchedNotAllowed(", ".join(errors))

    def in_timerange(self, timerange, now=None):
        """
        Validate if <now> is in <timerange>.
        """
        try:
            begin = self._time_to_minutes(timerange["begin"])
            end = self._time_to_minutes(timerange["end"])
            now = self._time_to_minutes(now)
        except:
            raise SchedNotAllowed("conversion error in timerange challenge")

        if begin <= end:
            if now >= begin and now <= end:
                return
        elif begin > end:
            #
            #     =================
            #     23h     0h      1h
            #
            if (now >= begin and now <= 1440) or \
               (now >= 0 and now <= end):
                return
        raise SchedNotAllowed("not in timerange %s-%s" % \
                              (timerange["begin"], timerange["end"]))

    def get_last(self, fname):
        """
        Return the last task run timestamp, fetched from the on-disk cache.
        """
        timestamp_f = self.get_timestamp_f(fname)
        if not os.path.exists(timestamp_f):
            return
        try:
            with open(timestamp_f, 'r') as ofile:
                buff = ofile.read()
            last = datetime.datetime.strptime(buff, "%Y-%m-%d %H:%M:%S.%f"+os.linesep)
            return last
        except (OSError, IOError, ValueError):
            return

    def in_timerange_interval(self, timerange, fname=None, now=None, last=None):
        """
        Validate if the last task run is old enough to allow running again.
        """
        if timerange["interval"] == 0:
            raise SchedNotAllowed("interval set to 0")
        if last is None:
            return
        if self._skip_action_interval(last, timerange["interval"], now=now):
            raise SchedNotAllowed("last run is too soon")
        return

    def _in_schedule(self, schedule, fname=None, now=None, last=None):
        """
        Validate if <now> is in the allowed days and in the allowed timranges.
        """
        self._in_days(schedule, now=now)
        delay = self._in_timeranges(schedule, fname=fname, now=now, last=last)
        return delay

    def in_schedule(self, schedules, fname=None, now=None, last=None):
        """
        Validate if <now> pass the constraints of a set of schedules,
        iterating over each non-excluded one.
        """
        if len(schedules) == 0:
            raise SchedNotAllowed("no schedule")
        errors = []
        for schedule in schedules:
            try:
                delay = self._in_schedule(schedule, fname=fname, now=now, last=last)
                if schedule["exclude"]:
                    raise SchedExcluded('excluded by schedule member "%s"' % schedule["raw"])
                else:
                    return delay
            except SchedNotAllowed as exc:
                errors.append(str(exc))
        raise SchedNotAllowed(", ".join(errors))

    def sched_get_schedule_raw(self, section, option):
        """
        Read the old/new style schedule options of a configuration file
        section. Convert if necessary and return the new-style formatted
        string.
        """
        if option is None:
            raise SchedNoDefault

        try:
            schedule_s = self.obj.oget(section, option if section == "DEFAULT" else "schedule")
        except ValueError:
            # keyword not found
            schedule_s = None
        if schedule_s is not None:
            # explicit schedule in config data
            pass
        elif self.svc and section in self.svc.resources_by_id and \
             hasattr(self.svc.resources_by_id[section], "default_schedule"):
            # driver default
            schedule_s = self.svc.resources_by_id[section].default_schedule
        elif option in self.config_defaults:
            # scheduler action default
            if section == "sync#i0":
                schedule_s = self.config_defaults["sync#i0_schedule"]
            else:
                schedule_s = self.config_defaults[option]
        else:
            raise SchedNoDefault

        return schedule_s

    def _in_days(self, schedule, now=None):
        self._sched_validate_month(schedule["month"], now=now)
        self._sched_validate_week(schedule["week"], now=now)
        self._sched_validate_day(schedule["day"], now=now)

    def _sched_validate_day(self, day, now=None):
        """
        Split the allowed <day> spec and for each element,
        validate if <now> is in allowed <day> of week and of month.
        """
        for _day in day.split(","):
            try:
                self.__sched_validate_day(_day, now=now)
                return
            except SchedNotAllowed:
                pass
        raise SchedNotAllowed("not in allowed days")

    def __sched_validate_day(self, day, now=None):
        """
        Validate if <now> is in allowed <day> of week and of month.
        """
        n_col = day.count(":")
        day_of_month = None
        from_tail = None
        from_head = None
        if n_col > 1:
            raise SchedSyntaxError("only one ':' allowed in day spec '%s'" %day)
        elif n_col == 1:
            day, day_of_month = day.split(":")
            if len(day_of_month) == 0:
                raise SchedSyntaxError("day_of_month specifier is empty")
            if day_of_month in ("first", "1st"):
                from_head = True
                day_of_month = 1
            elif day_of_month in ("second", "2nd"):
                from_head = True
                day_of_month = 2
            elif day_of_month in ("third", "3rd"):
                from_head = True
                day_of_month = 3
            elif day_of_month in ("fourth", "4th"):
                from_head = True
                day_of_month = 4
            elif day_of_month in ("fifth", "5th"):
                from_head = True
                day_of_month = 5
            elif day_of_month == "last":
                from_tail = True
                day_of_month = 1
            elif day_of_month[0] == "-":
                from_tail = True
                day_of_month = day_of_month[1:]
            elif day_of_month[0] == "+":
                from_head = True
                day_of_month = day_of_month[1:]
            try:
                day_of_month = int(day_of_month)
            except ValueError:
                raise SchedSyntaxError("day_of_month is not a number")

        day = self._sched_expand_value(day)

        if day in ("*", ""):
            allowed_days = range(7)
        else:
            allowed_days = [d for d in day if d >= 0 and d <= 6]
        if now is None:
            now = datetime.datetime.now()
        this_week_day = now.weekday()

        if this_week_day not in allowed_days:
            raise SchedNotAllowed

        if day_of_month is not None:
            _day = now
            _month = _day.month
            if from_head is True:
                if day == "":
                    day1 = _day - datetime.timedelta(days=day_of_month)
                    day2 = _day - datetime.timedelta(days=day_of_month-1)
                else:
                    day1 = _day - datetime.timedelta(days=7*day_of_month)
                    day2 = _day - datetime.timedelta(days=7*(day_of_month-1))
                if day1.month == _month or day2.month != _month:
                    raise SchedNotAllowed
            elif from_tail is True:
                if day == "":
                    day1 = _day + datetime.timedelta(days=day_of_month)
                    day2 = _day + datetime.timedelta(days=day_of_month-1)
                else:
                    day1 = _day + datetime.timedelta(days=7*day_of_month)
                    day2 = _day + datetime.timedelta(days=7*(day_of_month-1))
                if day1.month == _month or day2.month != _month:
                    raise SchedNotAllowed
            elif _day.day != day_of_month:
                raise SchedNotAllowed

        return

    def _sched_validate_week(self, week, now=None):
        """
        Validate if <now> is in allowed <week>.
        """
        week = self._sched_expand_value(week)

        if week == "*":
            return

        allowed_weeks = [w for w in week if w >= 1 and w <= 53]
        if now is None:
            now = datetime.datetime.now()
        if now.isocalendar()[1] not in allowed_weeks:
            raise SchedNotAllowed("not in allowed weeks")
        return

    def _sched_validate_month(self, month, now=None):
        """
        Validate if <now> is in allowed <month>.
        """
        if month == "*":
            return

        allowed_months = set()
        for _month in month.split(","):
            ecount = _month.count("%")
            if ecount == 1:
                month_s, modulo_s = _month.split("%")
            elif ecount == 0:
                month_s = _month
                modulo_s = None
            else:
                raise SchedSyntaxError("only one '%%' allowed in month definition '%s'" % _month)


            if month_s in ("", "*"):
                _allowed_months = set(range(12))
            else:
                _allowed_months = self._sched_expand_value(month_s)
            if modulo_s is not None:
                _allowed_months &= self.__sched_validate_month(modulo_s)
            allowed_months |= _allowed_months

        if now is None:
            now = datetime.datetime.now()
        if now.month not in allowed_months:
            raise SchedNotAllowed("not in allowed months")
        return

    @staticmethod
    def __sched_validate_month(modulo):
        shift = 0
        n_plus = modulo.count("+")
        if n_plus > 1:
            raise SchedSyntaxError("only one '+' is allowed in modulo '%s'" % modulo)
        if n_plus == 1:
            modulo, shift = modulo.split("+")
        try:
            modulo = int(modulo)
        except ValueError:
            raise SchedSyntaxError("modulo '%s' is not a number" % modulo)
        try:
            shift = int(shift)
        except ValueError:
            raise SchedSyntaxError("shift '%s' is not a number" % shift)
        return set([m for m in range(1, 13) if (m + shift) % modulo == 0])

    @staticmethod
    def _sched_to_int(name):
        try:
            idx = int(name)
            return idx
        except ValueError:
            name = name.lower()
            if name not in CALENDAR_NAMES:
                raise SchedSyntaxError("unknown calendar name '%s'" % name)
            return CALENDAR_NAMES[name]

    def _sched_expand_value(self, spec):
        """
        Top level schedule definition parser.
        Split the definition into sub-schedules, and parse each one.
        """
        elements = set()
        if spec in ("*", ""):
            return spec
        subspecs = spec.split(",")
        for subspec in subspecs:
            n_dash = subspec.count("-")
            if n_dash > 1:
                raise SchedSyntaxError("only one '-' allowed in timerange '%s'" % spec)
            elif n_dash == 0:
                elements.add(self._sched_to_int(subspec))
                continue
            begin, end = subspec.split("-")
            begin = self._sched_to_int(begin)
            end = self._sched_to_int(end)
            _range = sorted([begin, end])
            elements |= set(range(_range[0], _range[1]+1))
        return elements

    def _interval_from_timerange(self, timerange):
        """
        Return a default interval from a timerange data structure.
        This interval is the timerange length in minute, plus one.
        """
        begin_m = self._time_to_minutes(timerange['begin'])
        end_m = self._time_to_minutes(timerange['end'])
        if begin_m > end_m:
            return 24 * 60 - begin_m + end_m + 1
        return end_m - begin_m + 1

    def _sched_parse_timerange(self, spec, section=None):
        """
        Return the list of timerange data structure parsed from the <spec>
        definition string.
        """
        min_tr_len = 1

        def parse_timerange(spec):
            if spec == "*" or spec == "":
                return {"begin": "00:00", "end": "23:59"}
            if "-" not in spec:
                spec = "-".join((spec, spec))
            try:
                begin, end = spec.split("-")
            except:
                raise SchedSyntaxError("split '%s' error" % spec)
            if begin.count(":") != 1 or \
               end.count(":") != 1:
                raise SchedSyntaxError("only one ':' allowed in timerange '%s' end" % spec)
            begin_m = self._time_to_minutes(begin)
            end_m = self._time_to_minutes(end)
            if begin_m == end_m:
                end_m += min_tr_len
                end = "%02d:%02d" % (end_m // 60, end_m % 60)
            return {"begin": begin, "end": end}

        if section and section.startswith("sync"):
            probabilistic = False
        else:
            probabilistic = True

        tr_list = []
        for _spec in spec.split(","):
            if len(_spec) == 0 or _spec == "*":
                tr_data = {
                    "probabilistic": probabilistic,
                    "begin": "00:00",
                    "end": "23:59",
                    "interval": 1441,
                }
                tr_list.append(tr_data)
                continue
            ecount = _spec.count("@")
            if ecount == 0:
                tr_data = parse_timerange(_spec)
                tr_data["interval"] = self._interval_from_timerange(tr_data)
                if tr_data["interval"] <= min_tr_len + 1:
                    tr_data["probabilistic"] = False
                else:
                    tr_data["probabilistic"] = probabilistic
                tr_list.append(tr_data)
                continue

            elements = _spec.split("@")
            ecount = len(elements)
            if ecount < 2:
                raise SchedSyntaxError("missing @<interval> in '%s'" % _spec)
            if ecount > 2:
                raise SchedSyntaxError("only one @<interval> allowed in '%s'" % _spec)
            tr_data = parse_timerange(elements[0])
            try:
                tr_data["interval"] = convert_duration(elements[1], _from="m", _to="m")
            except ValueError as exc:
                raise SchedSyntaxError("interval '%s' is not a valid duration expression: %s" % (elements[1], exc))
            tr_len = self._interval_from_timerange(tr_data)
            if tr_len <= min_tr_len + 1 or tr_data["interval"] < tr_len:
                probabilistic = False
            tr_data["probabilistic"] = probabilistic
            tr_list.append(tr_data)
        return tr_list

    def sched_get_schedule(self, section, option, schedules=None):
        """
        Return the list of schedule structures for the spec string passed
        as <schedules> or, if not passed, from the <section>.<option> value
        in the configuration file.
        """
        if schedules is None:
            schedules = self.sched_get_schedule_raw(section, option)
        try:
            schedules = json.loads(schedules)
        except:
            pass
        if is_string(schedules):
            schedules = [schedules]

        data = []
        for schedule in schedules:
            schedule_orig = schedule
            schedule = schedule.strip()
            if len(schedule) == 0:
                continue
            if schedule.startswith("!"):
                exclude = True
                schedule = schedule[1:].strip()
            else:
                exclude = False
            if len(schedule) == 0:
                continue
            elements = schedule.split()
            ecount = len(elements)
            if ecount == 1:
                _data = {
                    "timeranges": self._sched_parse_timerange(elements[0], section=section),
                    "day": "*",
                    "week": "*",
                    "month": "*",
                }
            elif ecount == 2:
                _tr, _day = elements
                _data = {
                    "timeranges": self._sched_parse_timerange(_tr, section=section),
                    "day": _day,
                    "week": "*",
                    "month": "*",
                }
            elif ecount == 3:
                _tr, _day, _week = elements
                _data = {
                    "timeranges": self._sched_parse_timerange(_tr, section=section),
                    "day": _day,
                    "week": _week,
                    "month": "*",
                }
            elif ecount == 4:
                _tr, _day, _week, _month = elements
                _data = {
                    "timeranges": self._sched_parse_timerange(_tr, section=section),
                    "day": _day,
                    "week": _week,
                    "month": _month,
                }
            else:
                raise SchedSyntaxError("invalid number of element, '%d' not in "
                                       "(1, 2, 3, 4)" % ecount)
            _data["exclude"] = exclude
            _data["raw"] = schedule_orig
            data.append(_data)
        return data

    def allow_action_schedule(self, section, option, fname=None, now=None, last=None):
        if option is None:
            return
        if now is None:
            now = datetime.datetime.now()
        try:
            schedule = self.sched_get_schedule(section, option)
            delay = self.in_schedule(schedule, fname=fname, now=now, last=last)
            return delay
        except SchedNoDefault:
            raise SchedNotAllowed("no schedule in section %s and no default "
                                  "schedule"%section)
        except SchedSyntaxError as exc:
            raise SchedNotAllowed("malformed parameter value: %s.schedule "
                                  "(%s)"%(section, str(exc)))

    def skip_action_schedule(self, section, option, fname=None, now=None, last=None):
        try:
            self.allow_action_schedule(section, option, fname=fname, now=now, last=last)
            return False
        except SchedExcluded:
            return True
        except Exception:
            return True
        return True

    def get_timestamp_f(self, fname, success=False):
        """
        Return the full path of the last run timestamp file with the <fname>
        basename.
        """
        if self.svc:
            timestamp_d = os.path.join(self.svc.var_d, "scheduler")
        else:
            timestamp_d = os.path.join(rcEnv.paths.pathvar, "node", "scheduler")
        fpath = os.path.join(timestamp_d, fname)
        if success:
            fpath += ".success"
        return fpath

    def validate_action(self, action, now=None, lasts=None):
        """
        Decide if the scheduler task can run, and return the concerned rids
        for multi-resources actions.

        The callers are responsible for catching AbortAction.
        """
        if isinstance(now, (int, float)):
            now = datetime.datetime.fromtimestamp(now)
        if not self._is_croned():
            return
        if action not in self.scheduler_actions:
            return
        if not isinstance(self.scheduler_actions[action], list):
            skip = self.skip_action(action, now=now, lasts=lasts)
            if skip is True:
                raise ex.AbortAction
            return skip
        data = self.skip_action(action, now=now, lasts=lasts)
        sched_options = data["keep"]
        if len(sched_options) == 0:
            raise ex.AbortAction
        return [option.section for option in sched_options], data["delay"]

    def action_timestamps(self, action, rids=None, success=False):
        sched_options = self.scheduler_actions[action]
        tsfiles = []
        if not isinstance(sched_options, list):
            tsfile = self.get_timestamp_f(sched_options.fname, success=success)
            tsfiles.append(tsfile)
        else:
            if rids is None:
                return
            for _so in sched_options:
                if not _so.section in rids:
                    continue
                tsfile = self.get_timestamp_f(_so.fname, success=success)
                tsfiles.append(tsfile)

        if len(tsfiles) == 0:
            return

        try:
            last = datetime.datetime.fromtimestamp(float(os.environ["OSVC_SCHED_TIME"]))
        except Exception as exc:
            last = datetime.datetime.now()

        for tsfile in tsfiles:
            self._timestamp(tsfile, last=last)

    def _is_croned(self):
        """
        Return True if the cron option is set.
        """
        return self.options.cron

    def skip_action(self, action, section=None, fname=None,
                    schedule_option=None, now=None, lasts=None):
        if action not in self.scheduler_actions:
            if not self._is_croned():
                return False
            return {"count": 0, "keep": [], "skip": []}

        if isinstance(self.scheduler_actions[action], list):
            data = {"count": 0, "keep": [], "skip": []}
            idx = 0
            for idx, sopt in enumerate(self.scheduler_actions[action]):
                skip = self._skip_action(
                    action, sopt,
                    section=section, fname=fname, schedule_option=schedule_option,
                    now=now,
                    lasts=lasts,
                )
                if skip is True:
                    data["skip"].append(sopt)
                else:
                    data["keep"].append(sopt)
                    if "delay" not in data or skip < data["delay"]:
                        data["delay"] = skip
            data["count"] = idx + 1
            return data

        else:
            sopt = self.scheduler_actions[action]
            return self._skip_action(
                action, sopt,
                section=section, fname=fname, schedule_option=schedule_option,
                now=now,
                lasts=lasts,
            )

    def _skip_action(self, action, sopt, section=None, fname=None,
                     schedule_option=None, now=None, lasts=None):
        if sopt.req_collector and not self.node.collector_env.dbopensvc:
            return True
        if section is None:
            section = sopt.section
        if fname is None:
            fname = sopt.fname
        if schedule_option is None:
            schedule_option = sopt.schedule_option

        last = self.get_last(fname)
        if fname:
            if lasts:
                try:
                    cluster_last = lasts[sopt.section][action]["last"]
                except (KeyError, ValueError):
                    cluster_last = 0
            else:
                cluster_last = 0
            if cluster_last:
                # Another node reports a more recent run (for a shared
                # resource, like a task).
                # Update our last run cache file, so avoid running the action
                # too soon after a takeover.
                cluster_last = datetime.datetime.fromtimestamp(cluster_last)
                if not last or cluster_last > last:
                    self._timestamp(sopt.fname, last=cluster_last)
                    last = cluster_last

        if not self._is_croned():
            # don't update the timestamp file
            return False

        # check if we are in allowed scheduling period
        try:
            delay = self.allow_action_schedule(section, schedule_option, fname=fname, now=now, last=last)
        except Exception as exc:
            return True

        return delay

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        if self.obj.cd is None:
            print("you are not allowed to print schedules", file=sys.stderr)
            raise ex.excError()
        if self.options.format is None:
            self._print_schedule_default()
            return
        data = self._print_schedule_data()
        if self.svc and not self.svc.options.single_service:
            # let the Node object do the formatting (for aggregation)
            return data
        # format ourself
        return self._print_schedule(data)

    @formatter
    def _print_schedule(self, data):
        """
        Display the scheduling table using the formatter specified in
        command line --format option.
        """
        return data

    def _print_schedule_default(self):
        """
        Print the scheduling table in normal or detailed mode.
        """
        from utilities.render.forest import Forest
        tree = Forest()
        head_node = tree.add_node()
        head_node.add_column("Action", color.BOLD)
        head_node.add_column("Last Run", color.BOLD)
        if self.options.verbose:
            head_node.add_column("Next Run", color.BOLD)
        head_node.add_column("Config Parameter", color.BOLD)
        head_node.add_column("Schedule Definition", color.BOLD)

        for data in self._print_schedule_data():
            node = head_node.add_node()
            node.add_column(data["action"], color.LIGHTBLUE)
            node.add_column(data["last_run"])
            if self.options.verbose:
                node.add_column(data["next_run"])
            node.add_column(data["config_parameter"])
            node.add_column(data["schedule_definition"])

        tree.out()

    def _print_schedule_data(self):
        """
        Return a list of dict of schedule information for all tasks.
        """
        data = []
        for action in sorted(self.scheduler_actions):
            data += self.__print_schedule_data(action)
        return data

    def __print_schedule_data(self, action):
        """
        Return a dict of a scheduled task, or list of dict of a task-set,
        containing schedule information.
        """
        data = []
        if isinstance(self.scheduler_actions[action], list):
            for sopt in self.scheduler_actions[action]:
                data += [self.___print_schedule_data(action, sopt)]
        else:
            sopt = self.scheduler_actions[action]
            data += [self.___print_schedule_data(action, sopt)]
        return data

    def ___print_schedule_data(self, action, sopt):
        """
        Return a dict of a scheduled task information.
        """
        section = sopt.section
        schedule_option = sopt.schedule_option
        fname = sopt.fname

        try:
            schedule_s = self.sched_get_schedule_raw(section, schedule_option)
        except SchedNoDefault:
            schedule_s = "anytime"
        except SchedSyntaxError:
            schedule_s = "malformed"
        if len(schedule_s) == 0:
            schedule_s = "-"

        timestamp_f = self.get_timestamp_f(fname)
        try:
            with open(timestamp_f, 'r') as ofile:
                last_s = ofile.read()
                last_s = last_s.split('.')[0]
        except (IOError, OSError):
            last_s = "-"

        if section != "DEFAULT":
            param = "schedule"
        else:
            param = schedule_option
        param = '.'.join((section, param))
        if self.options.verbose:
            result = self.get_next_schedule(action)
            if result["next_sched"]:
                next_s = result["next_sched"].strftime("%Y-%m-%d %H:%M")
            else:
                next_s = "-"
            return dict(
                action=action,
                last_run=last_s,
                next_run=next_s,
                config_parameter=param,
                schedule_definition=schedule_s
            )
        else:
            return dict(
                action=action,
                last_run=last_s,
                config_parameter=param,
                schedule_definition=schedule_s,
            )

    @staticmethod
    def _str_to_datetime(datetime_str):
        """
        Convert a %Y-%m-%d %H:%M formatted string to a datetime.
        """
        converted = datetime.datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
        return converted

    def test_schedule(self, schedule_s, date_s, expected, last_s):
        """
        Test if <date_s> passes <schedule_s> constraints and compares with the
        expected boolean result <expected>.
        Print a test report line.
        This method is used by the test_scheduler() testing function.
        """
        dtm = self._str_to_datetime(date_s)
        if last_s:
            last = self._str_to_datetime(last_s)
        else:
            last = None

        try:
            schedule = self.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        except SchedSyntaxError as exc:
            if expected == None:
                print("passed : schedule syntax error %s (%s)" % (repr(schedule_s), str(exc)))
                return True
            else:
                print("failed : schedule syntax error %s (%s)" % (repr(schedule_s), str(exc)))
                return False
        try:
            delay = self.in_schedule(schedule, fname=None, now=dtm, last=last)
            result = True
            result_s = ""
        except SchedSyntaxError as exc:
            if expected == None:
                print("passed : schedule syntax error %s (%s)" % (repr(schedule_s), str(exc)))
                return True
            else:
                print("failed : schedule syntax error %s (%s)" % (repr(schedule_s), str(exc)))
                return False
        except SchedNotAllowed as exc:
            result = False
            result_s = "("+str(exc)+")"

        if result == expected:
            check = "passed"
            ret = True
        else:
            check = "failed"
            ret = False

        print("%s : now '%s' last %-18s in schedule %-50s expected %s => result %s %s" % \
              (check, date_s, repr(last_s), repr(schedule_s), str(expected), str(result), result_s))

        return ret


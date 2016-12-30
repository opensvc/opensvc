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

import rcExceptions as ex
from rcGlobalEnv import rcEnv, Storage
from rcUtilities import is_string
from rcColor import formatter

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


def fork(func, args=None, kwargs=None, serialize=False, delay=300):
    """
    A fork daemonizing function.
    """
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    if os.fork() > 0:
        # return to parent execution
        return

    # separate the son from the father
    os.chdir('/')
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except:
        os._exit(1)

    obj = args[0]
    if obj.__class__.__name__ == "Compliance":
        if obj.svc:
            self = obj.svc
        else:
            self = obj.node
    else:
        # svc or node
        self = obj

    if self.sched.name == "node":
        title = "node."+func.__name__.lstrip("_")
    else:
        title = self.sched.name+"."+func.__name__.lstrip("_")

    if serialize:
        lockfile = title+".fork.lock"
        lockfile = os.path.join(rcEnv.pathlock, lockfile)

        from lock import lock, unlock
        try:
            lockfd = lock(lockfile=lockfile, timeout=0, delay=0)
            self.sched.sched_log(title, "lock acquired", "debug")
        except Exception:
            self.sched.sched_log(title, "task is already running", "warning")
            os._exit(0)

    # now wait for a random delay to not DoS the collector.
    if delay > 0 and self.sched.name == "node":
        delay = int(random.random()*delay)
        self.sched.sched_log(title, "delay %d secs to level database load"%delay, "debug")
        try:
            time.sleep(delay)
        except KeyboardInterrupt as exc:
            self.log.error(exc)
            os._exit(1)

    try:
        func(*args, **kwargs)
    except Exception as exc:
        if serialize:
            unlock(lockfd)
        self.log.error(exc)
        os._exit(1)

    if serialize:
        unlock(lockfd)
    os._exit(0)

def scheduler_fork(func):
    """
    A decorator that runs the decorated function in a detached
    subprocess if the cron option is set, else runs it inline.
    """
    def _func(*args, **kwargs):
        self = args[0]
        if self.options.cron:
            fork(func, args, kwargs, serialize=True, delay=59)
        else:
            func(*args, **kwargs)
    return _func

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
                 schedule_option="push_schedule"):
        self.section = section
        self.fname = fname
        if self.fname is None:
            self.fname = "node"+os.sep+"last_"+section+"_push"
        self.schedule_option = schedule_option

class Scheduler(object):
    """
    The scheduler class.

    The node and each service inherit an independent scheduler through
    this class.
    """
    def __init__(self, config_defaults=None, config=None, options=None,
                 scheduler_actions=None, log=None, name="node", svc=None):
        self.config_defaults = config_defaults
        self.config = config

        if scheduler_actions is None:
            self.scheduler_actions = {}
        else:
            self.scheduler_actions = scheduler_actions

        if options is None:
            self.options = Storage()
        else:
            self.options = options

        self.name = name
        self.svc = svc
        self.log = log
        self.sched_log_shut = False

    def sched_log(self, task, msg, level):
        """
        A logger wrapping method, used to log to the service or node
        sublogger dedicated to scheduling.
        """
        if self.sched_log_shut:
            return

        try:
            task = task.replace(self.name + ".", "")
        except:
            pass

        log = logging.getLogger(self.log.name+".scheduler")
        getattr(log, level)(SCHED_FMT % (task, msg))

    def get_next_schedule(self, action, _max=14400):
        """
        Iterate future dates in search for the next date validating
        <action> scheduling constraints.
        """
        self.sched_log_shut = True
        now = datetime.datetime.now()
        cron = self.options.cron
        self.options.cron = True
        for idx in range(_max):
            future_dt = now + datetime.timedelta(minutes=idx*10)
            data = self.skip_action(action, now=future_dt, deferred_write_timestamp=True)
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
        limit = last + datetime.timedelta(minutes=delay)
        if now < limit:
            return False
        else:
            return True

        # never reach here
        return True

    @staticmethod
    def sched_delay(delay=59):
        """
        Sleep for a random delay before executing the task, and after the
        scheduling constraints have been validated.
        """
        delay = int(random.random()*delay)
        try:
            time.sleep(delay)
        except KeyboardInterrupt:
            raise ex.excError("interrupted while waiting for scheduler delay")

    def sched_write_timestamp(self, sopt):
        """
        Iterate the scheduled tasks to update the last run timestamp.
        """
        if not isinstance(sopt, list):
            sopt = [sopt]
        for _sopt in sopt:
            self._timestamp(_sopt.fname)

    @staticmethod
    def _timestamp(timestamp_f):
        """
        Update the timestamp file <timestamp_f>.
        If <timestamp_f> if is not a fullpath, consider it parented to
        <pathvar>.
        Create missing parent directories if needed.
        """
        if not timestamp_f.startswith(os.sep):
            timestamp_f = os.path.join(rcEnv.pathvar, timestamp_f)
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d, 0o755)
        with open(timestamp_f, 'w') as ofile:
            ofile.write(str(datetime.datetime.now())+'\n')
        return True

    def _skip_action_interval(self, last, interval, now=None):
        """
        Return the negation of _need_action_interval()
        """
        return not self._need_action_interval(last, interval, now=now)

    def _in_timerange_probabilistic(self, timerange, now=None):
        """
        Validate a timerange constraint of a scheduled task, with an added
        failure probability decreasing with the remaining allowed window.

            proba
              ^
        100%  |
         75%  |XXX
         50%  |XXXX
         25%  |XXXXXX
          0%  ----|----|-> elapsed
             0%  50%  100%

        This algo is meant to level collector's load which peaks
        when all daily cron trigger at the same minute.
        """
        if not timerange.get("probabilistic", False):
            return

        try:
            begin = self._time_to_minutes(timerange["begin"])
            end = self._time_to_minutes(timerange["end"])
            now = self._time_to_minutes(now)
        except:
            raise SchedNotAllowed("time conversion error in probabilistic "
                                  "challenge")

        if begin > end:
            end += 1440
        if now < begin:
            now += 1440

        length = end - begin

        if length < 60:
            # no need to play this game on short allowed periods
            return

        if timerange["interval"] <= length:
            # don't skip if interval <= period length, because the user
            # expects the action to run multiple times in the period
            return

        length -= 11
        elapsed = now - begin
        elapsed_pct = min(100, int(100.0 * elapsed / length))

        if elapsed_pct < 50:
            # fixed skip proba for a perfect leveling on the first half-period
            proba = 100.0 - max(1, 1000.0 / length)
        else:
            # decreasing skip proba on the second half-period
            proba = 100.0 - min(elapsed_pct, 100)

        rnd = random.random() * 100.0

        if rnd >= proba:
            self.log.debug("win probabilistic challenge: %d, "
                           "over %d"%(rnd, proba))
            return

        raise SchedNotAllowed("lost probabilistic challenge: %d, "
                              "over %d"%(rnd, proba))

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
        Iterates multiple allowed timeranges, and switch between simple and
        probabilistic validation.
        """
        if len(schedule["timeranges"]) == 0:
            raise SchedNotAllowed("no timeranges")
        errors = []
        for timerange in schedule["timeranges"]:
            try:
                self.in_timerange(timerange, now=now)
                self.in_timerange_interval(timerange, fname=fname, now=now, last=last)
                if fname is not None:
                    # fname as None indicates we run in test mode
                    self._in_timerange_probabilistic(timerange, now=now)
                return
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
            last = datetime.datetime.strptime(buff, "%Y-%m-%d %H:%M:%S.%f\n")
            return last
        except (OSError, IOError, ValueError):
            return

    def in_timerange_interval(self, timerange, fname=None, now=None, last=None):
        """
        Validate if the last task run is old enough to allow running again.
        """
        if timerange["interval"] == 0:
            raise SchedNotAllowed("interval set to 0")
        if fname is None:
            # test mode
            return
        if last is None:
            last = self.get_last(fname)
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
        self._in_timeranges(schedule, fname=fname, now=now, last=last)

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
                self._in_schedule(schedule, fname=fname, now=now, last=last)
                if schedule["exclude"]:
                    raise SchedExcluded('excluded by schedule member "%s"' % schedule["raw"])
                else:
                    return
            except SchedNotAllowed as exc:
                errors.append(str(exc))
        raise SchedNotAllowed(", ".join(errors))

    def sched_convert_to_schedule(self, config, section, prefix=""):
        """
        Read and convert a deprecated schedule definition from a configuration
        file section, handle json-formatted lists, and finally return a
        current-style schedule string.
        """
        def get_val(param):
            if not config.has_section(section) or \
               (not config.has_option(section, param) and \
                not config.has_option(section, prefix+param)):
                # internal syncs
                config_defaults = config.defaults()
                val = config_defaults.get(prefix+param)
            elif config.has_option(section, prefix+param):
                val = config.get(section, prefix+param)
            else:
                val = config.get(section, param)
            return str(val)

        days_s = get_val("days")
        interval_s = get_val("interval")
        period_s = get_val("period")

        if days_s == "None" or interval_s == "None" or period_s == "None":
            return ""

        try:
            days = json.loads(days_s)
        except:
            self.log.error("invalid days schedule definition in section",
                           section, days_s, file=sys.stderr)
            return ""

        try:
            periods = json.loads(period_s)
            elements = []
            if is_string(periods[0]):
                periods = [periods]
            for period in periods:
                elements.append("%s-%s@%s" % (period[0], period[1], interval_s))
            period_s = ",".join(elements)
        except:
            self.log.error("invalid periods schedule definition in section",
                           section, file=sys.stderr)
            return ""
        buff = "%(period)s %(days)s" % dict(
            period=period_s,
            days=",".join(days),
        )
        return buff.strip()

    def sched_get_schedule_raw(self, section, option):
        """
        Read the old/new style schedule options of a configuration file
        section. Convert if necessary and return the new-style formatted
        string.
        """
        if option is None:
            raise SchedNoDefault

        config = self.config

        def has_old_schedule_options(config, section):
            """
            Return True if a configuration file section has a deprecated
            schedule definition keyword
            """
            if config.has_option(section, 'sync_days') or \
               config.has_option(section, 'sync_interval') or \
               config.has_option(section, 'sync_period'):
                return True
            if config.has_option(section, 'days') or \
               config.has_option(section, 'interval') or \
               config.has_option(section, 'period'):
                return True
            return False

        if config.has_section(section) and \
           config.has_option(section, 'schedule'):
            schedule_s = config.get(section, 'schedule')
        elif section.startswith("sync") and config.has_section(section) and \
             has_old_schedule_options(config, section):
            if section.startswith("sync"):
                prefix = "sync_"
            elif section.startswith("app"):
                prefix = "app_"
            else:
                prefix = ""
            schedule_s = self.sched_convert_to_schedule(config, section, prefix=prefix)
        elif section.startswith("sync") and not config.has_section(section) and (\
              'sync_days' in config.defaults() or \
              'sync_interval' in config.defaults() or \
              'sync_period' in config.defaults() \
             ):
            schedule_s = self.sched_convert_to_schedule(config, section, prefix="sync_")
        elif config.has_option('DEFAULT', option):
            schedule_s = config.get('DEFAULT', option)
        elif self.svc and section in self.svc.resources_by_id and \
             hasattr(self.svc.resources_by_id[section], "default_schedule"):
            schedule_s = self.svc.resources_by_id[section].default_schedule
        elif option in self.config_defaults:
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

        allowed_months = set([])
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
        elements = set([])
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
        return end_m - begin_m + 1

    def _sched_parse_timerange(self, spec, section=None):
        """
        Return the list of timerange data structure parsed from the <spec>
        definition string.
        """

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
            if begin_m > end_m:
                tmp = end
                end = begin
                begin = tmp
            elif begin_m == end_m:
                end_m += 10
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
            tr_data["probabilistic"] = probabilistic
            try:
                tr_data["interval"] = int(elements[1])
            except:
                raise SchedSyntaxError("interval '%s' is not a number" % elements[1])
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
            self.in_schedule(schedule, fname=fname, now=now, last=last)
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

    @staticmethod
    def get_timestamp_f(fname):
        """
        Return the full path of the last run timestamp file with the <fname>
        basename.
        """
        timestamp_f = os.path.realpath(os.path.join(rcEnv.pathvar, fname))
        return timestamp_f

    def _is_croned(self):
        """
        Return True if the cron option is set.
        """
        return self.options.cron

    def skip_action(self, action, section=None, fname=None,
                    schedule_option=None, now=None,
                    deferred_write_timestamp=False):
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
                    now=now, deferred_write_timestamp=deferred_write_timestamp
                )
                if skip:
                    data["skip"].append(sopt)
                else:
                    data["keep"].append(sopt)
            data["count"] = idx + 1
            return data

        else:
            sopt = self.scheduler_actions[action]
            return self._skip_action(
                action, sopt,
                section=section, fname=fname, schedule_option=schedule_option,
                now=now, deferred_write_timestamp=deferred_write_timestamp
            )

    def _skip_action(self, action, sopt, section=None, fname=None,
                     schedule_option=None, now=None,
                     deferred_write_timestamp=False):
        if section is None:
            section = sopt.section
        if fname is None:
            fname = sopt.fname
        if schedule_option is None:
            schedule_option = sopt.schedule_option

        def title():
            """
            Return a string to use as the task title in log entries.
            """
            buff = ".".join((self.name, action))
            if "#" in section:
                buff += "." + section
            return buff

        if not self._is_croned():
            # don't update the timestamp file
            return False

        # check if we are in allowed scheduling period
        try:
            self.allow_action_schedule(section, schedule_option, fname=fname, now=now)
        except Exception as exc:
            self.sched_log(title(), str(exc), "debug")
            return True

        self.sched_log(title(), "run task", "info")

        # update the timestamp file
        if not deferred_write_timestamp:
            timestamp_f = self.get_timestamp_f(fname)
            self._timestamp(timestamp_f)
            self.sched_log(title(), "last run timestamp updated", "debug")

        return False

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        if not hasattr(self, "config") or self.config is None:
            print("you are not allowed to print schedules", file=sys.stderr)
            raise ex.excError()
        if self.options.format is None:
            self._print_schedule_default()
            return
        data = self._print_schedule_data()
        if self.svc and len(self.svc.node.svcs) > 1:
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
        Print the scheduling table in normal or detailled mode.
        """
        if self.options.verbose:
            print_sched_fmt = "%(action)-21s  %(last_run)-21s  %(next_run)-19s  %(config_parameter)-24s  %(schedule_definition)s"
            print("action                 last run               next run             config parameter          schedule definition")
            print("------                 --------               --------             ----------------          -------------------")
        else:
            print_sched_fmt = "%(action)-21s  %(last_run)-21s  %(config_parameter)-24s  %(schedule_definition)s"
            print("action                 last run               config parameter          schedule definition")
            print("------                 --------               ----------------          -------------------")
        for data in self._print_schedule_data():
            print(print_sched_fmt % data)

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

    def test_schedule(self, schedule_s, date_s, expected):
        """
        Test if <date_s> passes <schedule_s> constraints and compares with the
        expected boolean result <expected>.
        Print a test report line.
        This method is used by the test_scheduler() testing function.
        """
        dtm = self._str_to_datetime(date_s)

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
            self.in_schedule(schedule, fname=None, now=dtm)
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

        print("%s : test '%s' in schedule %-50s expected %s => result %s %s" % \
              (check, date_s, repr(schedule_s), str(expected), str(result), result_s))

        return ret

def test_scheduler():
    """
    A exercizing function for the scheduler.
    """
    tests = [
        ("", "2015-02-27 10:00", False),
        ("@0", "2015-02-27 10:00", False),
        ("*@0", "2015-02-27 10:00", False),
        ("*", "2015-02-27 10:00", True),
        ("*@61", "2015-02-27 10:00", True),
        ("09:00-09:20", "2015-02-27 10:00", False),
        ("09:00-09:20@31", "2015-02-27 10:00", False),
        ("09:00-09:00", "2015-02-27 10:00", False),
        ("09:20-09:00", "2015-02-27 10:00", False),
        ("09:00", "2015-02-27 10:00", False),
        ("09:00-09:20", "2015-02-27 09:09", True),
        ("09:00-09:20@31", "2015-02-27 09:09", True),
        ("09:00-09:00", "2015-02-27 09:09", True),
        ("09:20-09:00", "2015-02-27 09:09", True),
        ("09:00", "2015-02-27 09:09", True),
        ("* fri", "2015-10-09 10:00", True),
        ("* fri", "2015-10-08 10:00", False),
        ("* *:last", "2015-01-30 10:00", True),
        ("* *:last", "2015-01-31 10:00", True),
        ("* *:-1", "2015-01-31 10:00", True),
        ("* *:-1", "2015-01-24 10:00", False),
        ("* *:-2", "2015-01-31 10:00", False),
        ("* :last", "2015-01-30 10:00", False),
        ("* :last", "2015-01-31 10:00", True),
        ("* :-1", "2015-01-31 10:00", True),
        ("* :-2", "2015-01-30 10:00", True),
        ("* :-2", "2015-01-31 10:00", False),
        ("* :-2", "2015-01-05 10:00", False),
        ("* :5", "2015-01-05 10:00", True),
        ("* :+5", "2015-01-05 10:00", True),
        ("* :fifth", "2015-01-05 10:00", True),
        ("* :5", "2015-01-06 10:00", False),
        ("* :+5", "2015-01-06 10:00", False),
        ("* :fifth", "2015-01-06 10:00", False),
        ("* * * jan", "2015-01-06 10:00", True),
        ("* * * jan-feb", "2015-01-06 10:00", True),
        ("* * * %2", "2015-01-06 10:00", False),
        ("* * * %2+1", "2015-01-06 10:00", True),
        ("* * * jan-feb%2", "2015-01-06 10:00", False),
        ("* * * jan-feb%2+1", "2015-01-06 10:00", True),
        ("18:00-18:59@60 wed", "2016-08-31 18:00", True),
        ("18:00-18:59@60 wed", "2016-08-30 18:00", False),
        ("23:00-23:59@61 *:first", "2016-09-01 23:00", True),
        # syntax errors
        ("23:00-23:59@61 *:first:*", "2016-09-01 23:00", None),
        ("23:00-23:59@61 *:", "2016-09-01 23:00", None),
        ("23:00-23:59@61 *:*", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * %2%3", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * %2+1+2", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * %foo", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * %2+foo", "2016-09-01 23:00", None),
        ("23:00-23:59@61 freday", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * junuary", "2016-09-01 23:00", None),
        ("23:00-23:59@61 * * %2%3", "2016-09-01 23:00", None),
        ("23:00-23:59-01:00@61", "2016-09-01 23:00", None),
        ("23:00-23:59:00@61 * * %2%3", "2016-09-01 23:00", None),
        ("23:00-23:59@61@10", "2016-09-01 23:00", None),
        ("23:00-23:59 * * * * *", "2016-09-01 23:00", None),
    ]
    sched = Scheduler()
    for test in tests:
        assert sched.test_schedule(*test)

if __name__ == "__main__":
    test_scheduler()

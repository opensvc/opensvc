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

import core.exceptions as ex
from env import Env
from utilities.storage import Storage
from utilities.render.color import formatter, color
from utilities.string import is_string
from utilities.converters import convert_duration

# ISO-8601 weeks. week one is the first week with thursday in year

SCHED_FMT = "%s: %s"
ALL_MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
ALL_WEEKDAYS = [1, 2, 3, 4, 5, 6, 7]
ALL_DAYS = [{"weekday": wd} for wd in ALL_WEEKDAYS]
ALL_WEEKS = list(range(1, 54))
DAY_SECONDS = 24 * 60 * 60
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
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
    "sun": 7,
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
    def __init__(self, message="", until=0):
        self.message = message
        self.until = until

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

    def __str__(self):
        return "<SchedOpts section=%s fname=%s schedule_option=%s>" % (
            self.section,
            self.fname,
            self.schedule_option,
        )

    __repr__ = __str__

def sched_action(func):
    """
    A decorator in charge of updating the scheduler tasks and subtasks
    timestamps.
    """
    def _func(self, action, options=None):
        if options is None:
            options = Storage()
        self.sched.configure(action=action)
        if action in self.sched.actions:
            self.sched.action_timestamps(action, options.rid)
        try:
            ret = func(self, action, options)
        except ex.AbortAction:
            # finer-grained locking can raise that to cancel the task
            return 0
        if ret == 0 and action in self.sched.actions:
            self.sched.action_timestamps(action, options.rid, success=True)
        return ret
    return _func

def contextualize_days(year, month, days):
    monthdays = days_in_month(year, month)
    for i, day in enumerate(days):
        monthday = day.get("monthday")
        if monthday is None:
            continue
        if monthday > 0:
            continue
        if abs(monthday) > monthdays:
            continue
        days[i]["monthday"] = monthdays + monthday + 1
    return days

def modulo_filter(l, modulo):
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
    return set([m for m in l if (m + shift) % modulo == 0])

def resolve_calendar_name(name):
    try:
        idx = int(name)
        return idx
    except ValueError:
        name = name.lower()
        if name not in CALENDAR_NAMES:
            raise SchedSyntaxError("unknown calendar name '%s'" % name)
        return CALENDAR_NAMES[name]

def parse_calendar_expression(spec):
    """
    Top level schedule definition parser.
    Split the definition into sub-schedules, and parse each one.
    """
    elements = set()
    if spec in ("*", ""):
        return spec
    subspecs = spec.split(",")
    for subspec in subspecs:
        n_dash = subspec[1:].count("-")
        if n_dash > 1:
            raise SchedSyntaxError("only one '-' allowed in range '%s'" % spec)
        elif n_dash == 0:
            elements.add(resolve_calendar_name(subspec))
            continue
        begin, end = subspec.split("-")
        begin = resolve_calendar_name(begin)
        end = resolve_calendar_name(end)
        _range = sorted([begin, end])
        elements |= set(range(_range[0], _range[1]+1))
    return elements

def days_in_month(year, month):
    first_day_this_month = datetime.date(year=year, month=month, day=1)
    first_day_next_month = datetime.date(year if month<12 else year+1, month % 12 + 1, 1)
    return (first_day_next_month - first_day_this_month).days

def time_to_seconds(dt_spec):
    """
    Convert a datetime or a %H:%M[:%S] formatted string to seconds.
    """
    if isinstance(dt_spec, datetime.datetime):
        dtm = dt_spec
        dt_spec = dtm.hour * 60 * 60 + dtm.minute * 60 + dtm.second
    else:
        try:
            if dt_spec.count(":") == 1:
                dt_spec += ":00"
            dtm = time.strptime(dt_spec, "%H:%M:%S")
        except:
            raise SchedSyntaxError("malformed time string: %s"%str(dt_spec))
        dt_spec = dtm.tm_hour * 3600 + dtm.tm_min * 60 + dtm.tm_sec
    return dt_spec

def seconds_to_hms(s):
    return (
        s // 3600,
        (s % 3600) // 60,
        s % 60
    )

def time_to_hms(s):
    return seconds_to_hms(time_to_seconds(s))

def seconds_to_time(s):
    return "%02d:%02d:%02d" % seconds_to_hms(s)

def interval_from_timerange(timerange):
    """
    Return a default interval from a timerange data structure.
    This interval is the timerange length in minute, plus one.
    """
    begin_s = time_to_seconds(timerange['begin'])
    end_s = time_to_seconds(timerange['end'])
    if begin_s > end_s:
        return DAY_SECONDS - begin_s + end_s + 1
    return end_s - begin_s + 1

def in_timerange_safe(timerange, now=None):
    try:
        in_timerange(timerange, now)
        return True
    except SchedNotAllowed:
        return False

def in_timerange(timerange, now=None):
    """
    Validate if <now> is in <timerange>.
    """
    try:
        begin = time_to_seconds(timerange["begin"])
        end = time_to_seconds(timerange["end"])
        now = time_to_seconds(now)
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
        if (now >= begin and now <= DAY_SECONDS) or \
           (now >= 0 and now <= end):
            return
    raise SchedNotAllowed("not in timerange %s-%s" % \
                          (timerange["begin"], timerange["end"]))


class Schedule(object):
    def __init__(self, schedule, probabilistic=False, last=None):
        self.schedule = self.normalize_schedule(schedule)
        self.probabilistic = probabilistic
        self.last = last

    @staticmethod
    def normalize_schedule(schedules):
        try:
            schedules = json.loads(schedules)
        except:
            pass
        if schedules in (None, "@0", ""):
            return []
        if isinstance(schedules, (list, tuple, set)):
            return schedules
        return [schedules]

    @property
    def data(self):
        data = []
        for schedule in self.schedule:
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
                    "timeranges": self.parse_timerange(elements[0]),
                    "day": ALL_DAYS,
                    "week": ALL_WEEKS,
                    "month": ALL_MONTHS,
                }
            elif ecount == 2:
                _tr, _day = elements
                _data = {
                    "timeranges": self.parse_timerange(_tr),
                    "day": self.parse_day(_day),
                    "week": ALL_WEEKS,
                    "month": ALL_MONTHS,
                }
            elif ecount == 3:
                _tr, _day, _week = elements
                _data = {
                    "timeranges": self.parse_timerange(_tr),
                    "day": self.parse_day(_day),
                    "week": self.parse_week(_week),
                    "month": ALL_MONTHS,
                }
            elif ecount == 4:
                _tr, _day, _week, _month = elements
                _data = {
                    "timeranges": self.parse_timerange(_tr),
                    "day": self.parse_day(_day),
                    "week": self.parse_week(_week),
                    "month": self.parse_month(_month),
                }
            else:
                raise SchedSyntaxError("invalid number of element, '%d' not in "
                                       "(1, 2, 3, 4)" % ecount)
            _data["exclude"] = exclude
            _data["raw"] = schedule_orig
            data.append(_data)
        return data

    def parse_day(self, day):
        """
        Convert to a list of <integer day of week>:<integer day of month>
        """
        l = []
        for e in day.split(","):
            l += self._parse_day(e)
        return l

    @staticmethod
    def _parse_day(day):
        n_col = day.count(":")
        day_of_month = None
        from_tail = None
        from_head = None
        if n_col > 1:
            raise SchedSyntaxError("only one ':' allowed in day spec '%s'" %day)
        elif n_col == 1:
            day, day_of_month = day.split(":")
            from_head = True
            if len(day_of_month) == 0:
                raise SchedSyntaxError("day_of_month specifier is empty")
            if day_of_month in ("first", "1st"):
                day_of_month = 1
            elif day_of_month in ("second", "2nd"):
                day_of_month = 2
            elif day_of_month in ("third", "3rd"):
                day_of_month = 3
            elif day_of_month in ("fourth", "4th"):
                day_of_month = 4
            elif day_of_month in ("fifth", "5th"):
                day_of_month = 5
            elif day_of_month == "last":
                day_of_month = -1
            try:
                day_of_month = int(day_of_month)
            except ValueError:
                raise SchedSyntaxError("day_of_month is not a number")

        day = parse_calendar_expression(day)
        if day in ("*", ""):
            day = ALL_WEEKDAYS
        allowed_days = [{"weekday": d, "monthday": day_of_month} for d in day if d in ALL_WEEKDAYS]
        return allowed_days

    @staticmethod
    def parse_week(week):
        """
        Convert to a list of integer weeks
        """
        week = parse_calendar_expression(week)
        if week == "*":
            return ALL_WEEKS
        return sorted([w for w in week if 1 <= w <= 53])

    @staticmethod
    def parse_month(month):
        """
        Convert to a list of integer months
        """
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
                _allowed_months = ALL_MONTHS
            else:
                _allowed_months = parse_calendar_expression(month_s)
            if modulo_s is not None:
                _allowed_months = modulo_filter(_allowed_months, modulo_s)
            allowed_months |= _allowed_months
        return sorted(list(allowed_months))

    def parse_timerange(self, spec):
        """
        Return the list of timerange data structure parsed from the <spec>
        definition string.
        """
        min_tr_len = 1

        def parse_timerange(spec):
            if spec == "*" or spec == "":
                return {"begin": "00:00:00", "end": "23:59:59"}
            if "-" not in spec:
                spec = "-".join((spec, spec))
            try:
                begin, end = spec.split("-")
            except:
                raise SchedSyntaxError("split '%s' error" % spec)
            begin_s = time_to_seconds(begin)
            end_s = time_to_seconds(end)
            if begin_s == end_s:
                end_s += min_tr_len
                end = seconds_to_time(end_s)
            return {"begin": begin, "end": end}

        probabilistic = self.probabilistic

        tr_list = []
        for _spec in spec.split(","):
            if len(_spec) == 0 or _spec == "*":
                tr_data = {
                    "probabilistic": probabilistic,
                    "begin": "00:00",
                    "end": "23:59",
                    "interval": DAY_SECONDS,
                }
                tr_list.append(tr_data)
                continue
            ecount = _spec.count("@")
            if ecount == 0:
                tr_data = parse_timerange(_spec)
                tr_data["interval"] = interval_from_timerange(tr_data)
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
                tr_data["interval"] = convert_duration(elements[1], _from="m", _to="s")
            except ValueError as exc:
                raise SchedSyntaxError("interval '%s' is not a valid duration expression: %s" % (elements[1], exc))
            tr_len = interval_from_timerange(tr_data)
            if tr_len <= min_tr_len + 1 or tr_data["interval"] < tr_len:
                probabilistic = False
            tr_data["probabilistic"] = probabilistic
            tr_list.append(tr_data)
        return tr_list

    def validate(self, now=None, last=None, schedules=None):
        """
        Validate if <now> pass the constraints of a set of schedules,
        iterating over each non-excluded one.
        """
        def _validate(schedule):
            """
            Validate if <now> is in the allowed days and in the allowed timranges.
            """
            _in_days(schedule)
            return _in_timeranges(schedule)

        def _in_days(schedule):
            _validate_month(schedule["month"])
            _validate_week(schedule["week"])
            _validate_day(schedule["day"])

        def _validate_day(day):
            """
            Split the allowed <day> spec and for each element,
            validate if <now> is in allowed <day> of week and of month.
            """
            now_weekday = now.isoweekday()
            now_monthday = now.day
            for _day in contextualize_days(now.year, now.month, day):
                try:
                    __validate_day(_day["weekday"], _day.get("monthday"), now_weekday=now_weekday, now_monthday=now_monthday)
                    return
                except SchedNotAllowed:
                    pass
            raise SchedNotAllowed("not in allowed days")

        def __validate_day(weekday, monthday, now_weekday, now_monthday):
            """
            Validate if <now> is in allowed <day> of week and of month.
            """
            if now_weekday != weekday:
                raise SchedNotAllowed
            if monthday is None:
                return
            if now_monthday != monthday:
                raise SchedNotAllowed
            return

        def _validate_week(week):
            """
            Validate if <now> is in allowed <week>.
            """
            if now.isocalendar()[1] not in week:
                raise SchedNotAllowed("not in allowed weeks")
            return

        def _validate_month(month):
            """
            Validate if <now> is in allowed <month>.
            """
            if now.month not in month:
                raise SchedNotAllowed("not in allowed months")
            return

        def _in_timeranges(schedule):
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
            for timerange in schedule["timeranges"]:
                try:
                    in_timerange(timerange, now=now)
                    if schedule.get("exclude"):
                        return _timerange_remaining(timerange)
                    _in_timerange_interval(timerange)
                    return _timerange_delay(timerange)
                except SchedNotAllowed as exc:
                    errors.append(str(exc))
            raise SchedNotAllowed(", ".join(errors))

        def _timerange_remaining(timerange):
            try:
                begin = time_to_seconds(timerange["begin"])
                end = time_to_seconds(timerange["end"])
                second = time_to_seconds(now)
            except:
                raise SchedNotAllowed("time conversion error delay eval")
            # day change in the timerange
            if begin > end:
                end += DAY_SECONDS
            if second < begin:
                second += DAY_SECONDS

            length = end - begin
            remaining = end - second

            return remaining

        def _timerange_delay(timerange):
            """
            Return a delay in seconds, compatible with the timerange.

            The daemon scheduler thread will honor this delay,
            executing the task only when expired.

            This algo is meant to level collector's load which peaks
            when tasks trigger at the same second on every nodes.
            """
            if not timerange.get("probabilistic", False):
                return 0

            try:
                begin = time_to_seconds(timerange["begin"])
                end = time_to_seconds(timerange["end"])
                second = time_to_seconds(now)
            except:
                raise SchedNotAllowed("time conversion error delay eval")

            # day change in the timerange
            if begin > end:
                end += DAY_SECONDS
            if second < begin:
                second += DAY_SECONDS

            length = end - begin
            remaining = end - second - 1

            if remaining < 1:
                # no need to delay for tasks with a short remaining valid time
                return 0

            if timerange["interval"] < length:
                # don't delay if interval < period length, because the user
                # expects the action to run multiple times in the period. And
                # '@<n>' interval-only schedule are already different across
                # nodes due to daemons not starting at the same moment.
                return 0

            rnd = random.random()

            return int(remaining*rnd)

        def _in_timerange_interval(timerange):
            """
            Validate if the last task run is old enough to allow running again.
            """
            if timerange["interval"] == 0:
                raise SchedNotAllowed("interval set to 0")
            if last is None:
                return
            if _skip_action_interval(timerange["interval"]):
                raise SchedNotAllowed("last run is too soon")
            return

        def _skip_action_interval(interval):
            """
            Return the negation of _need_action_interval()
            """
            return not _need_action_interval(interval)

        def _need_action_interval(delay=10):
            """
            Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval.
            """
            if delay == 0:
                return False
            if last is None:
                return True
            limit = last + datetime.timedelta(seconds=delay)
            return now >= limit


        schedules = schedules or self.data
        if len(schedules) == 0:
            raise SchedNotAllowed("no schedule")
        errors = []
        for schedule in schedules:
            try:
                delay = _validate(schedule)
                if schedule["exclude"]:
                    raise SchedExcluded('excluded by schedule member "%s"' % schedule["raw"], until=delay)
                else:
                    return delay
            except SchedNotAllowed as exc:
                errors.append(str(exc))
        raise SchedNotAllowed(", ".join(errors))


    def get_next(self, now=None, last=None):
        if not now:
            now = time.time()
        if isinstance(now, (int, float)):
            now = datetime.datetime.fromtimestamp(now)
        if isinstance(last, (int, float)):
            last = datetime.datetime.fromtimestamp(last)
        _next = None
        _interval = None
        last = last or self.last
        sdata = self.data
        excludes = [sd for sd in sdata if sd.get("exclude")]
        includes = [sd for sd in sdata if not sd.get("exclude")]
        for s in includes:
            __next, __interval = self._get_next(s, now, last, excludes)
            if _next is None or _next > __next:
                _next = __next
                _interval = __interval
        return _next, _interval

    def _get_next(self, s, now, last, excludes):
        def valid_day(weekday, monthday, days):
            return {"weekday": weekday, "monthday": None} in days or \
                   {"weekday": weekday, "monthday": monthday} in days or \
                   {"weekday": weekday} in days

        def exclude(d):
            try:
                self.validate(d, last=last, schedules=excludes)
                return 0
            except SchedExcluded as exc:
                return exc.until
            except SchedNotAllowed as exc:
                return 0

        class NextDay(Exception):
            pass

        class Drift(Exception):
            def __init__(self, hour=0, minute=0, second=0):
                self.hour = hour
                self.minute = minute
                self.second = second

        def daily(year, month, day, hour, minute, second):
            d = datetime.datetime(year=year, month=month, day=monthday, hour=hour, minute=minute, second=second)
            week = d.isocalendar()[1]
            if week not in s["week"]:
                raise NextDay
            weekday = d.isoweekday()
            if not valid_day(weekday, monthday, days):
                raise NextDay
            try:
                d, interval = self.get_timerange(s, d, last)
            except SchedNotAllowed:
                raise NextDay
            try:
                self.validate(d, last=last, schedules=excludes)
            except SchedNotAllowed as exc:
                pass
            except SchedExcluded as exc:
                if exc.until > 0:
                    d += datetime.timedelta(seconds=exc.until)
                    if year == d.year and month == d.month and monthday == d.day:
                        raise Drift(hour=d.hour, minute=d.minute, second=d.second)
                    raise NextDay
            return d, interval

        hour = now.hour
        minute = now.minute
        second = now.second
        for year in [now.year, now.year+1]:
            for month in s["month"]:
                if year == now.year and month < now.month:
                    continue
                days = contextualize_days(year, month, s["day"])
                if year == now.year and month == now.month:
                    first_day = now.day
                else:
                    first_day = 1
                for monthday in range(first_day, days_in_month(year, month) + 1):
                    while True:
                        try:
                            return daily(year, month, monthday, hour, minute, second)
                        except NextDay:
                            hour = 0
                            minute = 0
                            second = 0
                            break
                        except Drift as exc:
                            hour = exc.hour
                            minute = exc.minute
                            second = exc.second

        return None, None

    @staticmethod
    def get_timerange(s, d, last):
        def valid_interval(d, last, interval):
            if not last:
                return True
            if d - last >= datetime.timedelta(seconds=interval):
                return True
            return False

        # if the candidate date is inside timeranges, return (candidate, smallest interval)
        ranges = [tr for tr in s["timeranges"] if in_timerange_safe(tr, d)]
        for tr in sorted(ranges, key=lambda x: (x.get("interval"), x.get("begin", 0))):
            interval = tr.get("interval")
            if not valid_interval(d, last, interval):
                di = last + datetime.timedelta(seconds=interval)
                if in_timerange_safe(tr, di):
                    return di, interval
                raise SchedNotAllowed
            return d, interval

        # the candidate date is outside timeranges, return the closest range's (begin, interval)
        ref = "%02d:%02d:%02d" % (d.hour, d.minute, d.second)
        ranges = [tr for tr in s["timeranges"] if ref < tr.get("begin", 0)]
        for tr in sorted(ranges, key=lambda x: (x.get("begin", 0), x.get("interval"))):
            interval = tr.get("interval")
            hour, minute, second = time_to_hms(tr["begin"])
            d = d.replace(hour=hour, minute=minute, second=second)
            if not valid_interval(d, last, interval):
                di = last + datetime.timedelta(seconds=interval)
                if in_timerange_safe(tr, di):
                    return di, interval
                continue
            return d, tr.get("interval")

        raise SchedNotAllowed

    def __iter__(self):
        return ScheduleIterator(self)

class ScheduleIterator:
    def __init__(self, schedule):
        self._schedule = schedule
        self._index = 0
        self._now = datetime.datetime.now()

    def __next__(self):
        while True:
            _next, interval = self._schedule.get_next(self._now)
            if not _next:
                raise StopIteration
            self._now = _next + datetime.timedelta(seconds=interval)
            return _next

    next = __next__

class Scheduler(object):
    """
    The scheduler class.

    The node and each service inherit an independent scheduler through
    this class.
    """
    def __init__(self, config_defaults=None, node=None, options=None,
                 scheduler_actions=None, log=None, svc=None,
                 configure_method=None):
        self.config_defaults = config_defaults
        self.configured = False
        self.configure_method = configure_method
        self.scheduler_actions = scheduler_actions or {}
        self.options = Storage(options or {})
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

    def update(self, data):
        self.scheduler_actions.update(data)

    def reconfigure(self):
        self.configured = False

    def configure(self, *args, **kwargs):
        """
        Placeholder for post-instanciation configuration.
        """
        if self.configured:
            return
        if self.configure_method:
            getattr(self.obj, self.configure_method)(*args, **kwargs)
        self.configured = True

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

    def get_schedule_raw(self, section, option):
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
            return schedule_s

        if self.svc and section in self.svc.resources_by_id and \
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

    def get_schedule(self, section, option, schedules=None):
        """
        Return the list of schedule structures for the spec string passed
        as <schedules> or, if not passed, from the <section>.<option> value
        in the configuration file.
        """
        if schedules is None:
            schedules = self.get_schedule_raw(section, option)
        if section and section.startswith("sync"):
            probabilistic = False
        else:
            probabilistic = True
        return Schedule(schedules, probabilistic)

    def get_timestamp_f(self, fname, success=False):
        """
        Return the full path of the last run timestamp file with the <fname>
        basename.
        """
        if self.svc:
            timestamp_d = os.path.join(self.svc.var_d, "scheduler")
        else:
            timestamp_d = os.path.join(Env.paths.pathvar, "node", "scheduler")
        fpath = os.path.join(timestamp_d, fname)
        if success:
            fpath += ".success"
        return fpath

    @property
    def actions(self):
        return self.scheduler_actions

    def action_timestamps(self, action, rids=None, success=False):
        sched_options = self.scheduler_actions[action]
        tsfiles = []
        for _so in sched_options:
            if rids and not _so.section in rids:
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

    def print_schedule(self):
        """
        The 'print schedule' node and service action entrypoint.
        """
        if self.obj.cd is None:
            print("you are not allowed to print schedules", file=sys.stderr)
            raise ex.Error()
        data = self.print_schedule_data()
        if self.options.format is None:
            self.format_schedule(data)
            return
        if self.svc and not self.svc.options.single_service:
            # let the Node object do the formatting (for aggregation)
            return data
        # format ourself
        return self.format_schedule_data(data)

    @formatter
    def format_schedule_data(self, data):
        """
        Display the scheduling table using the formatter specified in
        command line --format option.
        """
        return data

    @staticmethod
    def format_schedule(data):
        """
        Print the scheduling table as a tree.
        """
        from utilities.render.forest import Forest
        tree = Forest()
        head_node = tree.add_node()
        head_node.add_column("Action", color.BOLD)
        head_node.add_column("Last Run", color.BOLD)
        head_node.add_column("Next Run", color.BOLD)
        head_node.add_column("Config Parameter", color.BOLD)
        head_node.add_column("Schedule Definition", color.BOLD)

        for _data in data:
            node = head_node.add_node()
            node.add_column(_data["action"], color.LIGHTBLUE)
            node.add_column(_data["last_run"])
            node.add_column(_data["next_run"])
            node.add_column(_data["config_parameter"])
            node.add_column(_data["schedule_definition"])

        tree.out()

    def print_schedule_data(self):
        """
        Return a list of dict of schedule information for all tasks.
        """
        self.configure()
        data = []
        now = datetime.datetime.now()
        for action in self.scheduler_actions:
            data += self._print_schedule_data(action, now)
        return data

    def _print_schedule_data(self, action, now):
        """
        Return a dict of a scheduled task, or list of dict of a task-set,
        containing schedule information.
        """
        data = []
        for sopt in self.scheduler_actions[action]:
            data += [self.__print_schedule_data(action, sopt, now)]
        return sorted(data, key=lambda x: x["config_parameter"])

    def __print_schedule_data(self, action, sopt, now):
        """
        Return a dict of a scheduled task information.
        """
        section = sopt.section
        schedule_option = sopt.schedule_option
        fname = sopt.fname

        try:
            schedule = self.get_schedule(section, schedule_option)
            schedule_s = " ".join(schedule.schedule)
        except SchedNoDefault:
            schedule_s = "anytime"
        except SchedSyntaxError:
            schedule_s = "malformed"
        if len(schedule_s) == 0:
            schedule_s = "-"

        try:
            last = self.get_last(fname)
            last_s = last.strftime("%Y-%m-%d %H:%M:%S")
        except (AttributeError, IOError, OSError):
            last_s = "-"
            last = None

        if section != "DEFAULT":
            param = "schedule"
        else:
            param = schedule_option
        param = '.'.join((section, param))

        data = dict(
            action=action,
            last_run=last_s,
            config_parameter=param,
            schedule_definition=schedule_s,
        )

        if schedule_s in ("-", "@0", "malformed"):
            result = None
        else:
            result = schedule.get_next(now, last=last)[0]
        if result:
            next_s = result.strftime("%Y-%m-%d %H:%M:%S")
        else:
            next_s = "-"
        data["next_run"] = next_s

        return data

if __name__ == '__main__':
    now = datetime.datetime.now()
    last = datetime.datetime.now()
#    s = Schedule("00:10-01:00@1,03:10-04:00@10,@1h sun-mon:last,fri:first,wed 10-40 2,jun-aug")
#    Schedule("18:00-19:00@10").validate(now.replace(hour=18, minute=51), last=now.replace(hour=18, minute=51))
#    Schedule("18:00-19:00@10").validate(now.replace(hour=15, minute=01))
#    s = Schedule(["14:00-21:00@10", "!12:00-18:55", "!19:10-20:00"])
    s = Schedule("@11s mon:last")
#    s = Schedule("")
    print(json.dumps(s.data, indent=4))
    for _ in range(10):
        last, interval = s.get_next(now=last, last=last)
        if not last:
            break
        print(last, last.strftime("%c"), interval)


from __future__ import print_function
import sys
import os
import stat
import datetime
import json
import time
import random

from rcGlobalEnv import rcEnv

sched_fmt = "[%s] %-45s %s"
print_sched_fmt = "%-20s  %-21s  %-24s  %s"

def fork(fn, args=[], kwargs={}, serialize=False, delay=300):
    if os.fork() > 0:
        """ return to parent execution
        """
        return

    """ separate the son from the father
    """
    os.chdir('/')
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)
    except:
        os._exit(1)

    self = args[0]
    if hasattr(self, "svcname") and self.svcname is not None:
        title = self.svcname+"."+fn.__name__.lstrip("_")
    else:
        title = "node."+fn.__name__.lstrip("_")

    if serialize:
        lockfile = title+".fork.lock"
        lockfile = os.path.join(rcEnv.pathlock, lockfile)

        from lock import lock, unlock
        try:
            fd = lock(lockfile=lockfile, timeout=0, delay=0)
            print(sched_fmt % ("fork", title, "lock acquired"))
        except Exception as e:
            print(sched_fmt % ("fork", title, "task is already running"))
            os._exit(0)

    # now wait for a random delay to not DoS the collector.
    if delay > 0 and not hasattr(self, "svcname"):
        import random
        import time
        delay = int(random.random()*delay)
        print(sched_fmt % ("fork", title, "delay %d secs to level database load"%delay))
        try:
            time.sleep(delay)
        except KeyboardInterrupt as e:
            print(e)
            os._exit(1)

    try:
        fn(*args, **kwargs)
    except Exception as e:
        if serialize:
            unlock(fd)
        print(e, file=sys.stderr)
        os._exit(1)

    if serialize:
        unlock(fd)
    os._exit(0)

def scheduler_fork(fn):
    def _fn(*args, **kwargs):
        self = args[0]
        if self.options.cron or (hasattr(self, "cron") and self.cron):
            fork(fn, args, kwargs, serialize=True, delay=59)
        else:
            fn(*args, **kwargs)
    return _fn

class SchedNotAllowed(Exception): 
    pass

class SchedNoDefault(Exception): 
    pass 
 
class SchedSyntaxError(Exception): 
    pass 
 
class SchedExcluded(Exception): 
    pass 
 
class SchedOpts(object): 
    def __init__(self, section, 
                 fname=None, 
                 schedule_option="push_schedule"): 
        self.section = section 
        self.fname = fname 
        if self.fname is None: 
            self.fname = "last_"+section+"_push" 
        self.schedule_option = schedule_option 

class Scheduler(object):
    def __init__(self):
        self.calendar_names = {
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
          "sunday": 6
        }

    def need_action_interval(self, timestamp_f, delay=10):
        """ Return False if timestamp is fresher than now-interval
            Return True otherwize.
            Zero is a infinite interval
        """
        if delay == 0:
            return False
        if not os.path.exists(timestamp_f):
            return True
        try:
            with open(timestamp_f, 'r') as f:
                d = f.read()
                last = datetime.datetime.strptime(d,"%Y-%m-%d %H:%M:%S.%f\n")
                limit = last + datetime.timedelta(minutes=delay)
                if datetime.datetime.now() < limit:
                    return False
                else:
                    return True
                f.close()
        except:
            return True

        # never reach here
        return True

    def sched_delay(self, delay=59):
        delay = int(random.random()*delay)
        try:
            time.sleep(delay)
        except KeyboardInterrupt as e:
            raise ex.excError("interrupted while waiting for scheduler delay")

    def sched_write_timestamp(self, so):
        if type(so) != list:
            so = [so]
        for _so in so:
            self.timestamp(_so.fname)

    def timestamp(self, timestamp_f):
        if not timestamp_f.startswith(os.sep):
            timestamp_f = os.path.join(rcEnv.pathvar, timestamp_f)
        timestamp_d = os.path.dirname(timestamp_f)
        if not os.path.isdir(timestamp_d):
            os.makedirs(timestamp_d, 0o755)
        with open(timestamp_f, 'w') as f:
            f.write(str(datetime.datetime.now())+'\n')
            f.close()
        return True

    def skip_action_interval(self, timestamp_f, interval):
        return not self.need_action_interval(timestamp_f, interval)

    def in_timerange_probabilistic(self, timerange, now=None):
        if not timerange.get("probabilistic", False):
            return

        try:
            begin = self.time_to_minutes(timerange["begin"])
            end = self.time_to_minutes(timerange["end"])
            now = self.time_to_minutes(now)
        except:
            raise SchedNotAllowed("time conversion error in probabilistic challenge")

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

        """
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
        if elapsed_pct < 50:
            # fixed skip proba for a perfect leveling on the first half-period
            p = 100.0 - max(1, 1000.0 / length)
        else:
            # decreasing skip proba on the second half-period
            p = 100.0 - min(elapsed_pct, 100)

        import random
        r = random.random()*100.0

        """
        print("begin:", begin)
        print("end:", end)
        print("now:", now)
        print("length:", length)
        print("elapsed:", elapsed)
        print("elapsed_pct:", elapsed_pct)
        print("p:", p)
        print("r:", r)
        """

        if r >= p:
            #print("win probabilistic challenge: %d, over %d"%(r, p))
            return

        raise SchedNotAllowed("lost probabilistic challenge: %d, over %d"%(r, p))

    def time_to_minutes(self, s):
        if type(s) == datetime.datetime:
            t = s
            s = t.hour * 60 + t.minute
        else:
            try:
                t = time.strptime(s, "%H:%M")
            except:
                raise Exception("malformed time string: %s"%str(s))
            s = t.tm_hour * 60 + t.tm_min
        return s

    def in_timeranges(self, schedule, fname=None, now=None):
        if len(schedule["timeranges"]) == 0:
            raise SchedNotAllowed("no timeranges")
        l = []
        for tr in schedule["timeranges"]:
            try:
                self.in_timerange(tr, now=now)
                self.in_timerange_interval(tr, fname=fname, now=now)
                if fname is not None:
                    # fname as None indicates we run in test mode
                    self.in_timerange_probabilistic(tr, now=now)
                return
            except SchedNotAllowed as e:
                l.append(str(e))
        raise SchedNotAllowed(", ".join(l))

    def in_timerange(self, timerange, now=None):
        try:
            begin = self.time_to_minutes(timerange["begin"])
            end = self.time_to_minutes(timerange["end"])
            now = self.time_to_minutes(now)
        except:
            raise SchedNotAllowed("conversion error in timerange challenge")

        if begin <= end:
            if now >= begin and now <= end:
                return
        elif begin > end:
            """
                  XXXXXXXXXXXXXXXXX
                  23h     0h      1h
            """
            if (now >= begin and now <= 1440) or \
               (now >= 0 and now <= end):
                return
        raise SchedNotAllowed("not in timerange %s-%s"%(timerange["begin"],timerange["end"]))

    def in_timerange_interval(self, timerange, fname=None, now=None):
        if timerange["interval"] == 0:
            raise SchedNotAllowed("interval set to 0")
        if fname is None:
            # test mode
            return
        timestamp_f = self.get_timestamp_f(fname)
        if self.skip_action_interval(timestamp_f, timerange["interval"]):
            raise SchedNotAllowed("last run is too soon")
        return

    def _in_schedule(self, schedule, fname=None, now=None):
        self.in_days(schedule, now=now)
        self.in_timeranges(schedule, fname=fname, now=now)

    def in_schedule(self, schedules, fname=None, now=None):
        if len(schedules) == 0:
            raise SchedNotAllowed("no schedule")
        l = []
        for schedule in schedules:
            try:
                self._in_schedule(schedule, fname=fname, now=now)
                if schedule["exclude"]:
                    raise SchedExcluded('excluded by schedule member "%s"' % schedule["raw"])
                else:
                    return
            except SchedNotAllowed as e:
                l.append(str(e))
        raise SchedNotAllowed(", ".join(l))

    def sched_convert_to_schedule(self, config, section, prefix=""):
        days_s = config.get(section, prefix+'days')
        period_s = config.get(section, prefix+'period')
        interval_s = config.get(section, prefix+'interval')
        try:
            days = json.loads(days_s)
        except:
            return ""
        try:
            periods = json.loads(period_s)
            l = []
            if type(periods[0]) in (str, unicode):
                periods = [periods]
            for p in periods:
                l.append("%s-%s@%s" % (p[0], p[1], interval_s))
            period_s = ",".join(l)
        except:
            pass
        s = "%(period)s %(days)s" % dict(
              period=period_s,
              days=",".join(days),
            )
        return s.strip()

    def sched_get_schedule_raw(self, section, option):
        if option is None:
            raise SchedNoDefault

        if hasattr(self, "config"):
            config = self.config
        elif hasattr(self, "svc"):
            config =  self.svc.config

        if config.has_section(section) and \
           config.has_option(section, 'schedule'):
            schedule_s = config.get(section, 'schedule')
        elif config.has_section(section) and \
             config.has_option(section, 'days') and \
             config.has_option(section, 'interval') and \
             config.has_option(section, 'period'):
            schedule_s = self.sched_convert_to_schedule(config, section)
        elif config.has_option('DEFAULT', option):
            schedule_s = config.get('DEFAULT', option)
        elif option in self.config_defaults:
            schedule_s = self.config_defaults[option]
        else:
            raise SchedNoDefault

        return schedule_s

    def in_days(self, schedule, now=None):
        self._sched_validate_month(schedule["month"], now=now)
        self._sched_validate_week(schedule["week"], now=now)
        self._sched_validate_day(schedule["day"], now=now)

    def _sched_validate_day(self, day, now=None):
        for s in day.split(","):
            try:
                self.__sched_validate_day(s, now=now)
                return
            except SchedNotAllowed:
                pass
        raise SchedNotAllowed("not in allowed days")

    def __sched_validate_day(self, day, now=None):
        n_col = day.count(":")
        day_of_month = None
        from_tail = None
        from_head = None
        if n_col > 1:
            raise SchedSyntaxError
        elif n_col == 1:
            day, day_of_month = day.split(":")
            if len(day_of_month) == 0:
                raise SchedSyntaxError
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
                raise SchedSyntaxError

        day = self.sched_expand_value(day)

        if day in ("*", ""):
            allowed_days = range(7)
        else:
            allowed_days = [ d for d in day if d >= 0 and d <= 6 ]
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
                    d1 = _day - datetime.timedelta(days=day_of_month)
                    d2 = _day - datetime.timedelta(days=day_of_month-1)
                else:
                    d1 = _day - datetime.timedelta(days=7*day_of_month)
                    d2 = _day - datetime.timedelta(days=7*(day_of_month-1))
                if d1.month == _month or d2.month != _month:
                    raise SchedNotAllowed
            elif from_tail is True:
                if day == "":
                    d1 = _day + datetime.timedelta(days=day_of_month)
                    d2 = _day + datetime.timedelta(days=day_of_month-1)
                else:
                    d1 = _day + datetime.timedelta(days=7*day_of_month)
                    d2 = _day + datetime.timedelta(days=7*(day_of_month-1))
                if d1.month == _month or d2.month != _month:
                    raise SchedNotAllowed
            elif _day.day != day_of_month:
                raise SchedNotAllowed

        return

    def _sched_validate_week(self, week, now=None):
        week = self.sched_expand_value(week)

        if week == "*":
            return

        allowed_weeks = [ w for w in week if w >= 1 and w <= 53 ]
        if now is None:
            now = datetime.datetime.now()
        this_week_day = now.weekday()
        if now.isocalendar()[1] not in allowed_weeks:
            raise SchedNotAllowed("not in allowed weeks")
        return

    def _sched_validate_month(self, month, now=None):
        if month == "*":
            return

        allowed_months = set([])
        for s in month.split(","):
            n = s.count("%")
            if n == 1:
                month_s, modulo_s = s.split("%")
            elif n == 0:
                month_s = s
                modulo_s = None
            else:
                raise SchedSyntaxError("malformed month definition")

            
            if month_s in ("", "*"):
                _allowed_months = set(range(12))
            else:
                _allowed_months = self.sched_expand_value(month_s)
            if modulo_s is not None:
                _allowed_months &= self.__sched_validate_month(modulo_s)
            allowed_months |= _allowed_months

        if now is None:
            now = datetime.datetime.now()
        this_week_day = now.weekday()
        if now.month not in allowed_months:
            raise SchedNotAllowed("not in allowed months")
        return

    def __sched_validate_month(self, modulo):
        shift = 0
        n_plus = modulo.count("+")
        if n_plus > 1:
            raise SchedSyntaxError
        if n_plus == 1:
            modulo, shift = modulo.split("+")
        try:
            modulo = int(modulo)
            shift = int(shift)
        except ValueError:
            raise SchedSyntaxError
        return set([ m for m in range(1,13) if (m + shift) % modulo == 0])

    def sched_to_int(self, s):
        try:
            i = int(s)
            return i
        except ValueError:
            s = s.lower()
            if s not in self.calendar_names:
                 raise SchedSyntaxError("unknown calendar name")
            return self.calendar_names[s]
            
    def sched_expand_value(self, s):
        v = set([])
        if s in ("*", ""):
            return s
        l = s.split(",")
        for e in l:
            n_dash = e.count("-")
            if n_dash > 1:
                raise SchedSyntaxError
            elif n_dash == 0:
                v.add(self.sched_to_int(e))
                continue
            begin, end = e.split("-")
            begin = self.sched_to_int(begin)
            end = self.sched_to_int(end)
            _range = sorted([begin, end])
            v |= set(range(_range[0], _range[1]+1))
        return v

    def interval_from_timerange(self, tr):
        begin_m = self.time_to_minutes(tr['begin'])
        end_m = self.time_to_minutes(tr['end'])
        return end_m - begin_m + 1

    def sched_parse_timerange(self, s, section=None):
        def parse_timerange(s):
            if s == "*" or s == "":
                return {"begin": "00:00", "end": "23:59"}
            if "-" not in s:
                s = "-".join((s,s))
            try:
                begin, end = s.split("-")
            except:
                raise SchedSyntaxError
            if begin.count(":") != 1 or \
               end.count(":") != 1:
                raise SchedSyntaxError
            begin_m = self.time_to_minutes(begin)
            end_m = self.time_to_minutes(end)
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

        tr = []
        for e in s.split(","):
            if len(e) == 0 or e == "*":
                d = {
                  "probabilistic": probabilistic,
                  "begin": "00:00",
                  "end": "23:59",
                  "interval": 1441
                }
                tr.append(d)
                continue
            n = e.count("@")
            if n == 0:
                d = parse_timerange(e)
                d["interval"] = self.interval_from_timerange(d)
                d["probabilistic"] = probabilistic
                tr.append(d)
                continue
            
            l = e.split("@")
            n = len(l)
            if n != 2:
                raise SchedSyntaxError
            d = parse_timerange(l[0])
            d["probabilistic"] = probabilistic
            try:
                d["interval"] = int(l[1])
            except:
                raise SchedSyntaxError
            tr.append(d)
        return tr

    def sched_get_schedule(self, section, option, now=None, schedules=None):
        if schedules is None:
            schedules = self.sched_get_schedule_raw(section, option)
        try:
            schedules = json.loads(schedules)
        except:
            pass
        if type(schedules) in (str, unicode):
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
            l = schedule.split()
            n = len(l)
            if n == 1:
                d = {
                 "timeranges": self.sched_parse_timerange(l[0], section=section),
                 "day": "*",
                 "week": "*",
                 "month": "*",
                }
            elif n == 2:
                _tr, _day = l
                d = {
                 "timeranges": self.sched_parse_timerange(_tr, section=section),
                 "day": _day,
                 "week": "*",
                 "month": "*",
                }
            elif n == 3:
                _tr, _day, _week = l
                d = {
                 "timeranges": self.sched_parse_timerange(_tr, section=section),
                 "day": _day,
                 "week": _week,
                 "month": "*",
                }
            elif n == 4:
                _tr, _day, _week, _month = l
                d = {
                 "timeranges": self.sched_parse_timerange(_tr, section=section),
                 "day": _day,
                 "week": _week,
                 "month": _month,
                }
            else:
                raise SchedSyntaxError
            d["exclude"] = exclude
            d["raw"] = schedule_orig
            data.append(d)
        return data

    def allow_action_schedule(self, section, option, fname=None, now=None):
        if option is None:
            return
        if now is None:
            now = datetime.datetime.now()
        try:
            schedule = self.sched_get_schedule(section, option)
            self.in_schedule(schedule, fname=fname, now=now)
        except SchedNoDefault:
            raise SchedNotAllowed("no schedule in section %s and no default schedule"%section)
        except SchedSyntaxError:
            raise SchedNotAllowed("malformed parameter value: %s.schedule"%section)

    def skip_action_schedule(self, section, option, fname=None, now=None):
        try:
            self.allow_action_schedule(section, option, fname=fname, now=now)
            return False
        except SchedExcluded:
            return True
        except:
            return True

    def get_timestamp_f(self, fname):
        timestamp_f = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'var', fname))
        return timestamp_f

    def skip_action(self, action, section=None, fname=None, schedule_option=None, cmdline_parm=None, now=None, verbose=True, deferred_write_timestamp=False):
        if type(self.scheduler_actions[action]) == list:
            data = {"count": 0, "keep": [], "skip": []}
            for i, so in enumerate(self.scheduler_actions[action]):
                if self._skip_action(action, so, section=section, fname=fname, schedule_option=schedule_option, cmdline_parm=cmdline_parm, now=now, verbose=verbose, deferred_write_timestamp=deferred_write_timestamp):
                    data["skip"].append(so)
                else:
                    data["keep"].append(so)
            data["count"] = i+1
            return data
        else:
            so = self.scheduler_actions[action]
            return self._skip_action(action, so, section=section, fname=fname, schedule_option=schedule_option, cmdline_parm=cmdline_parm, now=now, verbose=verbose)

    def _skip_action(self, action, so, section=None, fname=None, schedule_option=None, cmdline_parm=None, now=None, verbose=True, deferred_write_timestamp=False):
        if section is None:
            section = so.section
        if fname is None:
            fname = so.fname
        if schedule_option is None:
            schedule_option = so.schedule_option

        if hasattr(self, "svcname"):
            scheduler = self.svcname
        else:
            scheduler = "node"

        def err(msg):
            if not verbose:
                return
            print(sched_fmt % ("skip", title(), msg))

        def title():
            s = ".".join((scheduler, action))
            if "#" in section:
                s += "."+section
            return s

        if not self.options.cron and \
           (not hasattr(self, "cron") or not self.cron):
            # don't update the timestamp file
            return False

        # check if we are in allowed scheduling period
        try:
            self.allow_action_schedule(section, schedule_option, fname=fname, now=now)
        except Exception as e:
            err(str(e))
            return True

        # update the timestamp file
        if not deferred_write_timestamp:
            timestamp_f = self.get_timestamp_f(fname)
            self.timestamp(timestamp_f)

        print(sched_fmt % ("exec", title(), "timestamp updated"))
        return False

    def print_schedule(self):
        print(print_sched_fmt % ("action", "last run", "config parameter", "schedule definition"))
        print(print_sched_fmt % ("------", "--------", "----------------", "-------------------"))
        for a in sorted(self.scheduler_actions):
            self._print_schedule(a)

    def _print_schedule(self, a):
        if type(self.scheduler_actions[a]) == list:
            for so in self.scheduler_actions[a]:
                 self.__print_schedule(a, so)
        else:
            so = self.scheduler_actions[a]
            self.__print_schedule(a, so)

    def __print_schedule(self, a, so):
        section = so.section
        schedule_option = so.schedule_option
        fname = so.fname

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
            with open(timestamp_f, 'r') as f:
                last_s = f.read()
                last_s = last_s.split('.')[0]
        except:
            last_s = "-"

        if section != "DEFAULT":
            param = "schedule"
        else:
            param = schedule_option
        param = '.'.join((section, param))
        print(print_sched_fmt % (a, last_s, param, schedule_s))

    def str_to_datetime(self, s):
        d = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M")
        return d

    def test_schedule(self, schedule_s, date_s, expected):
        d = self.str_to_datetime(date_s)
        
        try:
            schedule = self.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        except SchedSyntaxError:
            print("failed : schedule syntax error %s" % repr(schedule_s))
            return
        try:
            self.in_schedule(schedule, fname=None, now=d)
            result = True
            result_s = ""
        except SchedSyntaxError:
            print("failed : schedule syntax error %s" % repr(schedule_s))
            return
        except SchedNotAllowed as e:
            result = False
            result_s = "("+str(e)+")"
        if result == expected:
            check = "passed"
        else:
            check = "failed"
        print("%s : test '%s' in schedule %-50s expected %s => result %s %s" % (check, date_s, repr(schedule_s), str(expected), str(result), result_s))

if __name__ == "__main__":
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
    ]
    sched = Scheduler()
    for test in tests:
        sched.test_schedule(*test)
        

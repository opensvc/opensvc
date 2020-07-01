from datetime import datetime

import pytest

from core.node import Node
from core.scheduler import Scheduler, SchedNotAllowed, SchedSyntaxError


def to_datetime(datetime_str):
    """
    Convert a %Y-%m-%d %H:%M formatted string to a datetime.
    """
    converted = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    return converted


@pytest.fixture()
def scheduler():
    return Scheduler(node=Node())


@pytest.mark.ci
class TestSchedules:
    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s', [
        ("*", "2015-02-27 10:00"),
        ("*@61", "2015-02-27 10:00"),
        ("09:20-09:00", "2015-02-27 10:00"),
        ("09:00-09:20", "2015-02-27 09:09"),
        ("09:00-09:20@31", "2015-02-27 09:09"),
        ("* fri", "2015-10-09 10:00"),
        ("* *:last", "2015-01-30 10:00"),
        ("* *:last", "2015-01-31 10:00"),
        ("* *:-1", "2015-01-31 10:00"),
        ("* :last", "2015-01-31 10:00"),
        ("* :-1", "2015-01-31 10:00"),
        ("* :-2", "2015-01-30 10:00"),
        ("* :5", "2015-01-05 10:00"),
        ("* :+5", "2015-01-05 10:00"),
        ("* :fifth", "2015-01-05 10:00"),
        ("* * * jan", "2015-01-06 10:00"),
        ("* * * jan-feb", "2015-01-06 10:00"),
        ("* * * %2+1", "2015-01-06 10:00"),
        ("* * * jan-feb%2+1", "2015-01-06 10:00"),
        ("18:00-18:59@60 wed", "2016-08-31 18:00"),
        ("23:00-23:59@61 *:first", "2016-09-01 23:00"),
        ("23:00-23:59", "2016-09-01 23:00"),
        ("23:00-00:59", "2016-09-01 23:00"),
    ])
    def test_return_positive_delay_when_now_is_in_schedules(scheduler, schedule_s, date_s):
        schedules = scheduler.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        now = to_datetime(date_s)
        assert scheduler.in_schedule(schedules, fname=None, now=now) >= 0

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s, last_s', [
        ("@10", "2015-02-27 10:00", "2015-02-27 08:59"),
    ])
    def test_return_positive_delay_when_in_now_in_schedules_and_last_enough_old(scheduler, schedule_s, date_s, last_s):
        schedules = scheduler.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        now = to_datetime(date_s)
        last = to_datetime(last_s)
        assert scheduler.in_schedule(schedules, fname=None, now=now, last=last) >= 0

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s', [
        ("", "2015-02-27 10:00"),
        ("@0", "2015-02-27 10:00"),
        ("*@0", "2015-02-27 10:00"),
        ("09:00-09:20", "2015-02-27 10:00"),
        ("09:00-09:20@31", "2015-02-27 10:00"),
        ("09:00-09:00", "2015-02-27 10:00"),
        ("09:00", "2015-02-27 10:00"),
        ("09:00-09:00", "2015-02-27 09:09"),
        ("09:00", "2015-02-27 09:09"),
        ("09:20-09:00", "2015-02-27 09:09"),
        ("* fri", "2015-10-08 10:00"),
        ("* *:-1", "2015-01-24 10:00"),
        ("* *:-2", "2015-01-31 10:00"),
        ("* :last", "2015-01-30 10:00"),
        ("* :-2", "2015-01-31 10:00"),
        ("* :-2", "2015-01-05 10:00"),
        ("* :5", "2015-01-06 10:00"),
        ("* :+5", "2015-01-06 10:00"),
        ("* :fifth", "2015-01-06 10:00"),
        ("* * * %2", "2015-01-06 10:00"),
        ("* * * jan-feb%2", "2015-01-06 10:00"),
    ])
    def test_raise_sched_not_allowed_when_now_is_not_in_schedules(scheduler, schedule_s, date_s):
        schedules = scheduler.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        now = to_datetime(date_s)
        with pytest.raises(SchedNotAllowed):
            scheduler.in_schedule(schedules, fname=None, now=now)

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s, last_s', [
        ("@10", "2015-02-27 10:00", "2015-02-27 09:52"),
    ])
    def test_raise_sched_not_allowed_when_last_it_not_enough_old(scheduler, schedule_s, date_s, last_s):
        schedules = scheduler.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
        now = to_datetime(date_s)
        last = to_datetime(last_s)
        with pytest.raises(SchedNotAllowed):
            scheduler.in_schedule(schedules, fname=None, now=now, last=last)

    @staticmethod
    @pytest.mark.parametrize('schedule_s,', [
        "23:00-23:59@61 *:first:*",
        "23:00-23:59@61 *:",
        "23:00-23:59@61 *:*",
        "23:00-23:59@61 * * %2%3",
        "23:00-23:59@61 * * %2+1+2",
        "23:00-23:59@61 * * %foo",
        "23:00-23:59@61 * * %2+foo",
        "23:00-23:59@61 freday",
        "23:00-23:59@61 * * junuary",
        "23:00-23:59@61 * * %2%3",
        "23:00-23:59-01:00@61",
        "23:00-23:59:00@61 * * %2%3",
        "23:00-23:59@61@10",
        "23:00-23:02 mon 1 12 4",
        "21-22 mon 1 12",
        ["10:00-11:00", "14-15"],  # mix valid and invalid time range
        ["14-15", "10:00-11:00"],  # mix valid and invalid time range
    ])
    def test_it_detect_invalid_schedule_definitions(scheduler, schedule_s):
        with pytest.raises(SchedSyntaxError):
            schedules = scheduler.sched_get_schedule("dummy", "dummy", schedules=schedule_s)
            scheduler.in_schedule(schedules, fname=None, now=datetime.now())

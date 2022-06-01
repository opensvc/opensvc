from datetime import datetime

import pytest

from core.scheduler import Schedule, SchedNotAllowed, parse_calendar_expression

SCHEDULES_NOT_ALLOWED = [
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
    ("* *:-2", "2015-01-24 10:00"),
    ("* *:-2", "2015-01-31 10:00"),
    ("* :last", "2015-01-30 10:00"),
    ("* :-2", "2015-01-31 10:00"),
    ("* :-2", "2015-01-05 10:00"),
    ("* :5", "2015-01-06 10:00"),
    ("* :+5", "2015-01-06 10:00"),
    ("* :fifth", "2015-01-06 10:00"),
    ("* * * %2", "2015-01-06 10:00"),
    ("* * * jan-feb%2", "2015-01-06 10:00"),

    # :monthday 0, can't match anything
    ("* :0", "2015-01-01 10:00"),
    ("* :0", "2015-01-02 10:00"),
    ("* :0", "2015-01-30 10:00"),
    ("* :0", "2015-01-31 10:00"),
    ("* *:0", "2015-01-01 10:00"),
    ("* *:0", "2015-01-02 10:00"),
    ("* *:0", "2015-01-30 10:00"),
    ("* *:0", "2015-01-31 10:00"),
]

SCHEDULES_NOT_ENOUGH_OLD = [
    ("@10", "2015-02-27 10:00", "2015-02-27 09:52"),
    ("@10s", "2015-02-27 10:00:15", "2015-02-27 10:00:08"),
]

SCHEDULES_ALLOWED = [
    ("*", "2015-02-27 10:00"),
    ("*@61", "2015-02-27 10:00"),
    ("09:20-09:00", "2015-02-27 10:00"),
    ("09:00-09:20", "2015-02-27 09:09"),
    ("~09:00-09:20", "2015-02-27 09:09"),
    ("09:00-09:20@31", "2015-02-27 09:09"),
    ("* fri", "2015-10-09 10:00"),

    ("* *:fifth", "2015-01-05 10:00"),
    ("* *:fourth", "2015-01-04 10:00"),
    ("* *:third", "2015-01-03 10:00"),
    ("* *:second", "2015-01-02 10:00"),
    ("* *:first", "2015-01-01 10:00"),
    ("* *:1st", "2015-01-01 10:00"),
    ("* *:last", "2015-01-31 10:00"),
    ("* :last", "2015-01-31 10:00"),
    ("* *:-1", "2015-01-31 10:00"),

    ("* :fifth", "2015-01-05 10:00"),
    ("* :fourth", "2015-01-04 10:00"),
    ("* :third", "2015-01-03 10:00"),
    ("* :second", "2015-01-02 10:00"),
    ("* :first", "2015-01-01 10:00"),
    ("* :1st", "2015-01-01 10:00"),
    ("* :last", "2015-01-31 10:00"),

    ("* :5", "2015-01-05 10:00"),
    ("* :4", "2015-01-04 10:00"),
    ("* :3", "2015-01-03 10:00"),
    ("* :2", "2015-01-02 10:00"),
    ("* :1", "2015-01-01 10:00"),
    ("* :-1", "2015-01-31 10:00"),
    ("* :-2", "2015-01-30 10:00"),
    ("* :-3", "2015-01-29 10:00"),
    ("* :-4", "2015-01-28 10:00"),

    ("* *:5", "2015-01-05 10:00"),
    ("* *:4", "2015-01-04 10:00"),
    ("* *:3", "2015-01-03 10:00"),
    ("* *:2", "2015-01-02 10:00"),
    ("* *:1", "2015-01-01 10:00"),
    ("* *:-1", "2015-01-31 10:00"),
    ("* *:-2", "2015-01-30 10:00"),
    ("* *:-3", "2015-01-29 10:00"),
    ("* *:-4", "2015-01-28 10:00"),

    ("* * * jan", "2015-01-06 10:00"),
    ("* * * jan-feb", "2015-01-06 10:00"),
    ("* * * %2+1", "2015-01-06 10:00"),
    ("* * * jan-feb%2+1", "2015-01-06 10:00"),
    ("18:00-18:59@60 wed", "2016-08-31 18:00"),
    ("23:00-23:59@61 *:first", "2016-09-01 23:00"),
    ("23:00-23:59", "2016-09-01 23:00"),
    ("23:00-00:59", "2016-09-01 23:00"),
    ("@10", "2015-02-27 10:00"),
]

SCHEDULES_EXPECTED_INTERVAL = [
    ("@3s", 3),
    ("*@6s", 6),
    ("*@06s", 6),
    ("*@18s", 18),
    ("10:00-18:00@10s", 10)
]

SCHEDULES_VALID = [schedule[0] for schedule in SCHEDULES_ALLOWED + SCHEDULES_NOT_ENOUGH_OLD + SCHEDULES_NOT_ALLOWED +
                   SCHEDULES_EXPECTED_INTERVAL]


def to_datetime(datetime_str):
    """
    Convert a %Y-%m-%d %H:%M formatted string to a datetime.
    """
    converted = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
    return converted


@pytest.mark.ci
class TestSchedule(object):
    @staticmethod
    @pytest.mark.parametrize('schedule_s', SCHEDULES_VALID)
    def test_has_valid_schedule_definition(schedule_s):
        print("assert Schedule(%s).data is list" % schedule_s)
        assert isinstance(Schedule(schedule_s).data, list)

    @staticmethod
    @pytest.mark.parametrize('schedule_s, expected_interval', SCHEDULES_EXPECTED_INTERVAL)
    def test_has_valid_schedule_definition(schedule_s, expected_interval):
        data = Schedule(schedule_s).data
        assert data[0]['timeranges'][0]['interval'] == expected_interval

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s', SCHEDULES_ALLOWED)
    def test_return_positive_delay_when_now_is_in_schedules(
            schedule_s,
            date_s):
        schedule = Schedule(schedule_s)
        now = to_datetime(date_s)
        assert schedule.validate(now,
                                 to_datetime("1902-04-01 23:10")) >= 0
        assert schedule.validate(now) >= 0

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s, last_s', SCHEDULES_NOT_ENOUGH_OLD)
    def test_raise_not_allowed_when_last_is_not_enough_old(
            schedule_s,
            date_s,
            last_s):
        schedule = Schedule(schedule_s)
        with pytest.raises(SchedNotAllowed, match="last run is too soon"):
            schedule.validate(to_datetime(date_s),
                              to_datetime(last_s))

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s', SCHEDULES_NOT_ALLOWED)
    def test_raise_sched_not_allowed_when_now_is_not_in_schedules(schedule_s, date_s):
        schedule = Schedule(schedule_s)
        with pytest.raises(SchedNotAllowed):
            schedule.validate(now=to_datetime(date_s),
                              last=to_datetime(date_s))

    @staticmethod
    @pytest.mark.parametrize('schedule_s, date_s, last_s', [
        ("@10", "2015-02-27 10:00", "2015-02-27 09:52"),
    ])
    def test_raise_not_allowed_when_last_is_not_enough_old(
            schedule_s,
            date_s,
            last_s):
        schedule = Schedule(schedule_s)
        _ = schedule.data
        with pytest.raises(SchedNotAllowed, match="last run is too soon"):
            schedule.validate(to_datetime(date_s),
                              to_datetime(last_s))

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
        "14-15",
    ])
    def test_it_drop_invalid_schedule_definitions_from_data(
            schedule_s):
        expected_data = [
            {
                'timeranges': [{'probabilistic': False, 'begin': '09:00', 'end': '09:20', 'interval': 3600}],
                'day': [{'weekday': 1, 'monthday': 1},
                        {'weekday': 2, 'monthday': 1},
                        {'weekday': 3, 'monthday': 1},
                        {'weekday': 4, 'monthday': 1},
                        {'weekday': 5, 'monthday': 1},
                        {'weekday': 6, 'monthday': 1},
                        {'weekday': 7, 'monthday': 1}],
                'week': [1],
                'month': [1],
                'raw': '09:00-09:20@60 :1st 1 january', 'exclude': False}
        ]
        assert Schedule([schedule_s, "09:00-09:20@60 :1st 1 january"]).data == expected_data
        assert Schedule(["09:00-09:20@60 :1st 1 january", schedule_s]).data == expected_data
        assert Schedule(schedule_s).data == []

    @staticmethod
    @pytest.mark.parametrize("spec, expected", [
        ["mon", {1}],
        ["monday", {1}],
        ["sun", {7}],
        ["sunday", {7}],
        ["monday-sunday", {1, 2, 3, 4, 5, 6, 7}],
        ["monday-wed", {1, 2, 3}],
        ["sun-mon", {1, 7}],
        ["sun-tue", {1, 2, 7}],
        ["sun-wed", {1, 2, 3, 7}],
        ["mon-monday", {1}],
        ["tuesday-tue", {2}],
        ["sun-fri", {1, 2, 3, 4, 5, 7}],
    ])
    def test_parse_calendar_expression_day(spec, expected):
        assert parse_calendar_expression(spec, 7) == expected

    @staticmethod
    @pytest.mark.parametrize("spec, expected", [
        ["jan", {1}],
        ["january", {1}],
        ["jan-apr", {1, 2, 3, 4}],
        ["nov-feb", {1, 2, 11, 12}],
        ["nov-jan", {1, 11, 12}],
    ])
    def test_parse_calendar_expression_month(spec, expected):
        assert parse_calendar_expression(spec, 12) == expected

    @staticmethod
    @pytest.mark.parametrize("spec, expected", [
        ["1", {1}],
        ["1-41", set(range(1, 42))],
        ["1-53", set(range(1, 54))],
        ["8-53", set(range(8, 54))],
        ["50-2", {1, 2, 50, 51, 52, 53}],

    ])
    def test_parse_calendar_expression_week(spec, expected):
        assert parse_calendar_expression(spec, 53) == expected

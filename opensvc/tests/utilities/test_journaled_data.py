import sys
from copy import deepcopy

import pytest

# noinspection PyUnresolvedReferences
from foreign.six.moves import queue
from utilities.journaled_data import JournaledData, JournaledDataView


TEST_SCENARIO = [
    # (function_name, kwargs, expected_result or exception, expected_patch_details),
    ("set", dict(path=None, value={"a": {"b": 0, "c": [1, 2], "d": {"da": ""}}}), None,
     [{"id": 1, "data": [[["a"], {"b": 0, "c": [1, 2], "d": {"da": ""}}]]}]),

    ("get_copy", dict(path=["a", "c"]), [1, 2],
     []),

    ("set", dict(path=["a"], value={"b": 1, "c": [1, 2, 3], "e": {"ea": 1, "eb": 2}}), None,
     [{"id": 2, "data": [[["a", "b"], 1], [["a", "c", 2], 3], [["a", "e"], {"ea": 1, "eb": 2}], [["a", "d"], ]]}],
     ),

    ("set", dict(path=["a", "b"], value=2), None,
     [{"id": 3, "data": [[["a", "b"], 2]]}]),

    ("set", dict(path=["a", "d"], value=["f"]), None,
     [{"id": 4, "data": [[["a", "d"], ["f"]]]}]),

    ("get", dict(path=["a"]), {"b": 2, "c": [1, 2, 3], "e": {"ea": 1, "eb": 2}, "d": ["f"]},
     []),

    ("inc", dict(path=["a", "d"]), 1,
     [{"id": 5, "data": [[["a", "d"], 1]]}]),

    ("inc", dict(path=["a", "d"]), 2,
     [{"id": 6, "data": [[["a", "d"], 2]]}]),

    ("inc", dict(path=["a", "d"]), 3,
     [{"id": 7, "data": [[["a", "d"], 3]]}]),

    ("exists", dict(path=["a", "b"]), True,
     []),

    ("exists", dict(), True,
     []),

    ("unset", dict(path=["a", "b"]), None,
     [{"id": 8, "data": [[["a", "b"]]]}]),

    ("unset_safe", dict(path=["a", "not-here"]), None,
     []),

    ("unset", dict(path=["a", "not-here"]), KeyError("not-here"),
     []),

    ("exists", dict(path=["a", "b"]), False,
     []),

    ("inc", dict(path=["a", "new"]), 1,
     [{"id": 9, "data": [[["a", "new"], 1]]}]),

    ("inc", dict(path=["new1", "new2"]), KeyError("new1"),
     []),

    ("keys", dict(path=["a"]), ["c", "e", "d", "new"],
     []),

    ("keys", dict(path=["a", "e"]), ["ea", "eb"],
     []),

    ("keys", dict(path=["does-not-exists"]), KeyError("does-not-exists"),
     []),

    ("get", dict(path=["does-not-exists"]), KeyError("does-not-exists"),
     []),

    ("get", dict(path=["does-not-exists"], default="some default"), "some default",
     []),

    ("keys_safe", dict(path=["does-not-exists"]), [],
     []),

    ("setnx", dict(path=["a", "d"], value=["can not be set"]), None,
     []),

    ("merge", dict(path=["a", "e"], value={"ec": "EC", "ed": "ED"}), None,
     [{"id": 10, "data": [[["a", "e", "ec"], "EC"]]},
      {"id": 11, "data": [[["a", "e", "ed"], "ED"]]},
      ]),

    ("set", dict(path=["a", "c"], value=[1]), None,
     [{"id": 12, "data": [[['a', 'c', 2]], [['a', 'c', 1]]]}]),

    ("set", dict(path=["a", "c"], value=[1, 2, 3]), None,
     [{"id": 13, "data": [[['a', 'c', 1], 2], [['a', 'c', 2], 3]]}]),

]

EXPECTED_FINAL_DATA = {"a": {"c": [1, 2, 3], "e": {"ea": 1, "eb": 2, "ec": "EC", "ed": "ED"}, "d": 3, "new": 1}}

EXPECTED_CHANGES = [[['a'], {'b': 0, 'c': [1, 2], 'd': {'da': ''}}],
                    [['a', 'b'], 1],
                    [['a', 'c', 2], 3],
                    [['a', 'e'], {'ea': 1, 'eb': 2}],
                    [['a', 'd']],
                    [['a', 'b'], 2],
                    [['a', 'd'], ['f']],
                    [['a', 'd'], 1],
                    [['a', 'd'], 2],
                    [['a', 'd'], 3],
                    [['a', 'b']],
                    [['a', 'new'], 1],
                    [['a', 'e', 'ec'], 'EC'],
                    [['a', 'e', 'ed'], 'ED'],
                    [['a', 'c', 2]],
                    [['a', 'c', 1]],
                    [['a', 'c', 1], 2],
                    [['a', 'c', 2], 3]]

EXPECTED_CHANGES_FROM_A = [[[], {'b': 0, 'c': [1, 2], 'd': {'da': ''}}],
                           [['c', 2], 3],
                           [['e'], {'ea': 1, 'eb': 2}],
                           [['d']],
                           [['d'], ['f']],
                           [['d'], 1],
                           [['d'], 2],
                           [['d'], 3],
                           [['new'], 1],
                           [['e', 'ec'], 'EC'],
                           [['e', 'ed'], 'ED'],
                           [['c', 2]],
                           [['c', 1]],
                           [['c', 1], 2],
                           [['c', 2], 3]]


def run(data, check_events):
    for fn, kwargs, expected_result, expected_events in TEST_SCENARIO:
        print("* %s(%s)" % (fn, ", ".join(["%s=%s" % (k, v) for k, v in kwargs.items()])))
        if fn in ['get_copy'] and isinstance(data, JournaledDataView):
            continue
        if isinstance(expected_result, Exception):
            with pytest.raises(expected_result.__class__, match=str(expected_result)):
                getattr(data, fn)(**kwargs)
            print("raise %s" % expected_result.__repr__())
        else:
            result = getattr(data, fn)(**kwargs)
            if getattr(data, 'event_q', None):
                if fn == 'keys' and int(sys.version[0]) < 3:
                    result_sorted = deepcopy(result)
                    result_sorted.sort()
                    expected_result_sorted = deepcopy(expected_result)
                    expected_result_sorted.sort()
                    assert result_sorted == expected_result_sorted, 'unexpeted sorted result'
                else:
                    assert result == expected_result, 'unexpected result'
            if result is not None:
                print("   => %s" % result)
            if getattr(data, 'event_q', None):
                events = []
                while data.event_q is not None and not data.event_q.empty():
                    msg = data.event_q.get(0)
                    print("   => event %s" % msg)
                    if msg.get("kind") == "patch":
                        del(msg["ts"])
                        del(msg["kind"])
                    events.append(msg)
                if check_events:
                    assert events == expected_events, 'unexpected event detected'
    if hasattr(data, 'dump_changes'):
        print("journal: %s" % data.dump_changes())
    if hasattr(data, 'dump_data'):
        print("data:    %s" % data.dump_data())


if int(sys.version[0]) < 3:
    check_events = [False]
    check_events_ids = ["without check events"]
else:
    check_events = [True, False]
    check_events_ids = ["with check events", "without check events"]


@pytest.mark.ci
@pytest.mark.parametrize('with_queue', [True, False], ids=["with queue", "without queue"])
@pytest.mark.parametrize('check_events', check_events, ids=check_events_ids)
class TestJournaledDataWithoutJournal(object):
    @staticmethod
    def test(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_data() == EXPECTED_FINAL_DATA
        assert data.dump_changes() == []


@pytest.mark.ci
@pytest.mark.parametrize('with_queue', [True, False], ids=["with queue", "without queue"])
@pytest.mark.parametrize('check_events', check_events, ids=check_events_ids)
class TestJournaledDataWithJournal(object):
    @staticmethod
    def test_with_full_journaling_has_expected_data(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=[], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_with_full_journaling_has_expected_journal(with_queue, check_events):
        if int(sys.version[0]) < 3:
            pytest.skip("skipped skip on python 2")
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=[], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_changes() == EXPECTED_CHANGES

    @staticmethod
    def test_with_full_journaling_can_apply_journal(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=[], event_q=event_q, emit_interval=0)
        run(data, check_events)
        rdata = JournaledData()
        rdata.patch(patchset=data.dump_changes())
        assert rdata.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_with_a_journaling_with_exclude_a_b_has_expected_data(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["a"], event_q=event_q, emit_interval=0, journal_exclude=[["b"]])
        run(data, check_events)
        assert data.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_with_a_journaling_with_exclude_a_b_has_expected_journal(with_queue, check_events):
        if int(sys.version[0]) < 3:
            pytest.skip("skipped skip on python 2")
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["a"], event_q=event_q, emit_interval=0, journal_exclude=[["b"]])
        run(data, check_events)
        assert data.dump_changes() == EXPECTED_CHANGES_FROM_A

    @staticmethod
    def test_with_a_journaling_with_exclude_a_b_can_apply_journal(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["a"], event_q=event_q, emit_interval=0, journal_exclude=[["b"]])
        run(data, check_events)
        rdata = JournaledData()
        rdata.patch(patchset=data.dump_changes())
        expected_patched_copy = deepcopy(EXPECTED_FINAL_DATA["a"])
        expected_patched_copy.update({"b": 0})
        assert rdata.dump_data() == expected_patched_copy

    @staticmethod
    def test_with_a_b_journaling_has_expected_data(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["a", "b"], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_with_a_b_journaling_expect_empty_journal(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["a", "b"], event_q=event_q, emit_interval=0)
        run(data, check_events)
        rdata = JournaledData()
        rdata.patch(patchset=data.dump_changes())
        assert rdata.dump_data() is None


@pytest.mark.ci
@pytest.mark.parametrize('with_queue', [True, False], ids=["with queue", "without queue"])
class TestJournaledDataView(object):
    @staticmethod
    def test_from_data_with_full_journaling(with_queue):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=[], event_q=event_q, emit_interval=0)
        data_view = JournaledDataView(data=data, path=[])
        run(data_view, False)
        assert data_view.get() == EXPECTED_FINAL_DATA
        assert data.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_from_data_without_journaling(with_queue):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(event_q=event_q, emit_interval=0)
        data_view = JournaledDataView(data=data, path=[])
        run(data_view, False)
        assert data_view.get() == EXPECTED_FINAL_DATA
        assert data.dump_data() == EXPECTED_FINAL_DATA

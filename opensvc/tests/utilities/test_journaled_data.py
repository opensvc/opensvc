import sys
from copy import deepcopy

import pytest

# noinspection PyUnresolvedReferences
from foreign.six.moves import queue
from utilities.journaled_data import JournaledData, JournaledDataView


TEST_SCENARIO = [
    # (function_name, kwargs, expected_result or exception, expected_patch_details or [expected_patch_details,])
    ("set", dict(path=None, value={"a": {"b": 0, "c": [1, 2], "d": {"da": ""}}}), None,
     [{"id": 1, "data": [[["a"], {"b": 0, "c": [1, 2], "d": {"da": ""}}]]}]),

    ("get_copy", dict(path=["a", "c"]), [1, 2],
     []),

    ("set", dict(path=["a"], value={"b": 1, "c": [1, 2, 3], "e": {"ea": 1, "eb": 2}}), None,
     [
         # need alternate values for non insertion guaranteed order
         [{"id": 2, "data": [[["a", "b"], 1], [["a", "c", 2], 3], [["a", "e"], {"ea": 1, "eb": 2}], [["a", "d"], ]]}],
         [{"id": 2, "data": [[["a", "b"], 1], [["a", "e"], {"ea": 1, "eb": 2}], [["a", "c", 2], 3], [["a", "d"], ]]}],

         [{"id": 2, "data": [[["a", "c", 2], 3], [["a", "b"], 1], [["a", "e"], {"ea": 1, "eb": 2}], [["a", "d"], ]]}],
         [{"id": 2, "data": [[["a", "c", 2], 3], [["a", "e"], {"ea": 1, "eb": 2}], [["a", "b"], 1], [["a", "d"], ]]}],

         [{"id": 2, "data": [[["a", "e"], {"ea": 1, "eb": 2}], [["a", "b"], 1], [["a", "c", 2], 3], [["a", "d"], ]]}],
         [{"id": 2, "data": [[["a", "e"], {"ea": 1, "eb": 2}], [["a", "c", 2], 3], [["a", "b"], 1], [["a", "d"], ]]}],
     ]),

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

    ("sorted_keys", dict(path=["a"]), ["c", "d", "e", "new"],
     []),

    ("keys", dict(path=["a", "e"]), ["ea", "eb"],
     []),

    ("sorted_keys", dict(path=["a", "e"]), ["ea", "eb"],
     []),

    ("keys", dict(path=["does-not-exists"]), KeyError("does-not-exists"),
     []),

    ("sorted_keys", dict(path=["does-not-exists"]), KeyError("does-not-exists"),
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
     [
         # need alternate values for non insertion guaranteed order
         [{"id": 10, "data": [[["a", "e", "ec"], "EC"]]},
          {"id": 11, "data": [[["a", "e", "ed"], "ED"]]}],
         [{"id": 10, "data": [[["a", "e", "ed"], "ED"]]},
          {"id": 11, "data": [[["a", "e", "ec"], "EC"]]}],
      ]),

    ("set", dict(path=["a", "c"], value=[1]), None,
     [{"id": 12, "data": [[['a', 'c', 2]], [['a', 'c', 1]]]}]),

    ("set", dict(path=["a", "c"], value=[1, 2, 3]), None,
     [{"id": 13, "data": [[['a', 'c', 1], 2], [['a', 'c', 2], 3]]}]),

    ("set", dict(path=["array"], value=[["00"], [], 2]), None,
     [{"id": 14, "data": [[['array'], [["00"], [], 2]]]}]),

    ("set", dict(path=["array", 1], value=["ONE"]), None,
     [{"id": 15, "data": [[['array', 1, 0], "ONE"]]}]),

    ("set", dict(path=["array", 1], value=["AAA"]), None,
     [{"id": 16, "data": [[['array', 1, 0], "AAA"]]}]),

    ("set", dict(path=["array", 1, 1], value="BBB"), None,
     [{"id": 17, "data": [[['array', 1, 1], "BBB"]]}]),
]

EXPECTED_FINAL_DATA = {"a": {"c": [1, 2, 3], "e": {"ea": 1, "eb": 2, "ec": "EC", "ed": "ED"}, "d": 3, "new": 1},
                       "array": [["00"], ["AAA", "BBB"], 2]}

EXPECTED_CHANGES = [
    [['a'], {'b': 0, 'c': [1, 2], 'd': {'da': ''}}],
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
    [['a', 'c', 2], 3],
    [['array'], [["00"], [], 2]],
    [['array', 1, 0], "ONE"],
    [['array', 1, 0], "AAA"],
    [['array', 1, 1], "BBB"],
]

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


if sys.version_info.major < 3 \
        or (sys.version_info.major == 3 and sys.version_info.minor < 7):
    skip_journal_check = 'skipped when guaranteed dict insertion order'
else:
    skip_journal_check = None


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
                if fn == 'keys':
                    result_sorted = deepcopy(result)
                    result_sorted.sort()
                    expected_result_sorted = deepcopy(expected_result)
                    expected_result_sorted.sort()
                    assert result_sorted == expected_result_sorted, 'unexpected sorted result'
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
                    if len(expected_events) > 0 and isinstance(expected_events[0], list):
                        # when multiple values are possible
                        if not skip_journal_check:
                            # can trust 1st value
                            assert events == expected_events[0], 'unexpected event detected'
                        else:
                            j = 0
                            for i, expected_events_element in enumerate(expected_events):
                                print('compared to %s' % expected_events_element)
                                if events == expected_events_element:
                                    j = i
                                    break
                            assert events == expected_events[j], 'unexpected event detected'
                    else:
                        assert events == expected_events, 'unexpected event detected'
    if hasattr(data, 'dump_changes'):
        print("journal: %s" % data.dump_changes())
    if hasattr(data, 'dump_data'):
        print("data:    %s" % data.dump_data())


check_events_values = [True, False]
check_events_ids = ["with check events", "without check events"]


@pytest.mark.ci
@pytest.mark.parametrize('with_queue', [True, False], ids=["with queue", "without queue"])
@pytest.mark.parametrize('check_events', check_events_values, ids=check_events_ids)
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
@pytest.mark.parametrize('check_events', check_events_values, ids=check_events_ids)
class TestJournaledDataWithJournal(object):
    @staticmethod
    def test_with_full_journaling_has_expected_data(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=[], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_data() == EXPECTED_FINAL_DATA

    @staticmethod
    def test_with_full_journaling_has_expected_journal(with_queue, check_events):
        if skip_journal_check:
            pytest.skip(skip_journal_check)
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
        if skip_journal_check:
            pytest.skip(skip_journal_check)
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

    @staticmethod
    def test_with_array_journaling_expected_journal(with_queue, check_events):
        if skip_journal_check:
            pytest.skip(skip_journal_check)
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["array", 1], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_changes() == [
            [[], []],
            [[0], "ONE"],
            [[0], "AAA"],
            [[1], "BBB"],
        ]

    @staticmethod
    def test_with_array_journaling_has_expected_data(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["array", 1], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.get_copy(path=["array", 1]) == EXPECTED_FINAL_DATA["array"][1]

    @staticmethod
    def test_with_array_journaling_can_apply_journal(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["array", 1], event_q=event_q, emit_interval=0)
        run(data, check_events)
        rdata = JournaledData()
        rdata.patch(patchset=data.dump_changes())
        assert rdata.dump_data() == data.get_copy(path=["array", 1])

    @staticmethod
    def test_with_array_journaling_expected_journal_with_exclude(with_queue, check_events):
        event_q = queue.Queue() if with_queue else None
        data = JournaledData(journal_head=["array", 1], journal_exclude=[[1]], event_q=event_q, emit_interval=0)
        run(data, check_events)
        assert data.dump_changes() == [
            [[], []],
            [[0], "ONE"],
            [[0], "AAA"],
        ]


@pytest.fixture(scope='function')
def data_with_journal_on_array_element():
    data = JournaledData(journal_head=["array", 0, 1])
    data.set(path=["array"], value=[
        ["0-0", "0-1", "0-2"],
        ["1-0", "1-1", "1-2"],
        ["2-0", "2-1", "2-2"],
    ])
    data.set(path=["array", 0, 1], value="update-0-1")
    data.set(path=["array", 0, 1], value="update-0-1-new")
    return data


@pytest.mark.ci
class TestJournaledDataWithJournalOnArrayElement(object):
    @staticmethod
    def test_has_expected_journal(data_with_journal_on_array_element):
        expected_changes = [
            [[], "0-1"],
            [[], "update-0-1"],
            [[], "update-0-1-new"],
        ]
        assert data_with_journal_on_array_element.dump_changes() == expected_changes

    @staticmethod
    def test_has_expected_data(data_with_journal_on_array_element):
        expected_data = {"array": [
            ["0-0", "update-0-1-new", "0-2"],
            ["1-0", "1-1", "1-2"],
            ["2-0", "2-1", "2-2"],
        ]}
        assert data_with_journal_on_array_element.dump_data() == expected_data

    @staticmethod
    def test_can_reproduce_data(data_with_journal_on_array_element):
        rdata = JournaledData()
        rdata.patch(patchset=data_with_journal_on_array_element.dump_changes())
        assert rdata.dump_data() == "update-0-1-new"


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


@pytest.mark.ci
class TestJournaledDataDumpChanges(object):
    @staticmethod
    @pytest.mark.parametrize('value', [[1, 2], {"one": 1}, 1])
    def test_update_of_dump_changes_result_must_not_change_journal(value):
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value=deepcopy(value))

        data.dump_changes()[0][1] = {}

        assert data.dump_changes()[0][1] == deepcopy(value)


@pytest.mark.ci
class TestJournaledDataGetFull(object):
    @staticmethod
    def test_can_apply_patch_after_full_installed():
        remote_data = JournaledData(journal_head=[])
        local_data = JournaledData(journal_head=[])
        remote_data.set(path=["a"], value={"one": 1, "two": 2})

        # remote diff sent => pop journal
        remote_data.pop_diff()

        remote_data.unset(path=["a", "one"])

        # full sent
        remote_data_full = remote_data.get_full()
        # full apply
        local_data.set(value=remote_data_full)

        # new updates
        remote_data.set(path=["a", "three"], value=3)

        # simulate diff sent
        remote_patch = remote_data.pop_diff()
        print('patch to apply: %s' % remote_patch)
        local_data.patch(patchset=remote_patch)

        assert remote_data.get_copy() == local_data.get_copy()

    @staticmethod
    def test_journal_is_empty_just_after_get_full_to_allow_apply_next_incr():
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value="up")
        assert data.dump_changes() == [[["a"], "up"]]
        data.get_full()
        assert data.dump_changes() == [], "journal is not prepared for next incremental"

    @staticmethod
    def test_get_full_return_data_from_path():
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value={"one": 1})
        assert data.get_full(path=["a"]) == {"one": 1}
        assert data.get_full(path=["a"]) == {"one": 1}

    @staticmethod
    def test_get_full_return_data_from_head_when_path_is_ommited():
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value={"one": 1})
        assert data.get_full() == {"a": {"one": 1}}


@pytest.mark.ci
class TestJournaledDataViewGetFull(object):
    @staticmethod
    def test_get_full_return_relative_view_when_path_is_ommited():
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value={"one": 1})
        view = JournaledDataView(data=data, path=["a"])
        assert view.get_full() == {"one": 1}


    @staticmethod
    def test_get_full_return_relative_view_from_arg_path():
        data = JournaledData(journal_head=[])
        data.set(path=["a"], value={"one": 1})
        view = JournaledDataView(data=data, path=["a"])
        assert view.get_full(path=["one"]) == 1

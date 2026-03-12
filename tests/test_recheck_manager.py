from recheck_manager import RecheckPair, RecheckQueuePlanner


def test_recheck_queue_planner_sender_mode_keeps_sender_groups():
    planner = RecheckQueuePlanner(
        mode="sender",
        shuffle_groups=False,
        shuffle_inside_group=False,
        seed=1,
    )
    planner.build(
        [
            RecheckPair(account_id=1, target_id=101, nickname="A"),
            RecheckPair(account_id=1, target_id=102, nickname="B"),
            RecheckPair(account_id=2, target_id=201, nickname="A"),
            RecheckPair(account_id=2, target_id=202, nickname="B"),
        ]
    )
    out = planner.pop_many(10)
    assert [p.account_id for p in out] == [1, 1, 2, 2]


def test_recheck_queue_planner_nickname_mode_groups_by_nickname():
    planner = RecheckQueuePlanner(
        mode="nickname",
        shuffle_groups=False,
        shuffle_inside_group=False,
        seed=1,
    )
    planner.build(
        [
            RecheckPair(account_id=1, target_id=101, nickname="NickA"),
            RecheckPair(account_id=2, target_id=201, nickname="NickB"),
            RecheckPair(account_id=3, target_id=301, nickname="NickA"),
            RecheckPair(account_id=4, target_id=401, nickname="NickB"),
        ]
    )
    out = planner.pop_many(10)
    nicks = [p.nickname for p in out]
    assert nicks[:2] == ["NickA", "NickA"]
    assert nicks[2:] == ["NickB", "NickB"]


def test_recheck_queue_planner_deduplicates_account_target_pair():
    planner = RecheckQueuePlanner(mode="sender", shuffle_groups=False, shuffle_inside_group=False, seed=1)
    planner.build(
        [
            RecheckPair(account_id=1, target_id=101, nickname="A"),
            RecheckPair(account_id=1, target_id=101, nickname="A"),
            RecheckPair(account_id=1, target_id=102, nickname="B"),
        ]
    )
    out = planner.pop_many(10)
    assert [(p.account_id, p.target_id) for p in out] == [(1, 101), (1, 102)]


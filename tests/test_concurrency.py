import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from anura.concurrent.manager import DeadlockException, LockManager
from anura.constants import LockMode


def catch_exception(test_func):
    def decorator(*args):
        try:
            test_func(*args)
        except DeadlockException:
            return True
        return False

    return decorator


@catch_exception
def single_op(mgr: LockManager, txn_id: int, locks: tuple[int], modes: tuple[LockMode]) -> None:
    with mgr.lock(locks[0], txn_id, modes[0]):
        time.sleep(0.1)


@catch_exception
def double_op(mgr: LockManager, txn_id: int, locks: tuple[int], modes: tuple[LockMode]) -> None:
    with mgr.lock(locks[0], txn_id, modes[0]):
        time.sleep(0.1)
        with mgr.lock(locks[1], txn_id, modes[1]):
            time.sleep(0.1)
        logging.info(f"TXN {txn_id} exit on LOCK {locks[1]}")
    logging.info(f"TXN {txn_id} exit on LOCK {locks[0]}")


@catch_exception
def triple_op(mgr: LockManager, txn_id: int, locks: tuple[int], modes: tuple[LockMode]) -> None:
    with mgr.lock(locks[0], txn_id, modes[0]):
        time.sleep(0.1)
        with mgr.lock(locks[1], txn_id, modes[1]):
            time.sleep(0.1)
            with mgr.lock(locks[2], txn_id, modes[2]):
                time.sleep(0.1)
            logging.info(f"TXN {txn_id} exit on LOCK {locks[2]}")
        logging.info(f"TXN {txn_id} exit on LOCK {locks[1]}")
    logging.info(f"TXN {txn_id} exit on LOCK {locks[0]}")


@pytest.mark.parametrize(
    "op1, locks1, modes1, op2, locks2, modes2, has_deadlock",
    [
        (single_op, (1,), (LockMode.EXCLUSIVE,), single_op, (1, 1), (LockMode.SHARED,), False),
        (single_op, (1,), (LockMode.EXCLUSIVE,), single_op, (1, 1), (LockMode.EXCLUSIVE,), False),
        (
            double_op,
            (1, 1),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            single_op,
            (1, 1),
            (LockMode.SHARED,),
            False,
        ),
        (
            double_op,
            (1, 1),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            single_op,
            (1, 1),
            (LockMode.SHARED,),
            False,
        ),
        (
            double_op,
            (1, 1),
            (LockMode.SHARED, LockMode.SHARED),
            single_op,
            (1, 1),
            (LockMode.SHARED,),
            False,
        ),
        (
            double_op,
            (1, 1),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            single_op,
            (1, 1),
            (LockMode.SHARED,),
            False,
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            True,
            id="1",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            True,
            id="2",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            True,
            id="3",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.SHARED),
            True,
            id="4",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            True,
            id="5",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            True,
            id="6",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            False,
            id="7",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.SHARED),
            False,
            id="8",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            True,
            id="9",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.EXCLUSIVE, LockMode.SHARED),
            False,
            id="10",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.SHARED, LockMode.EXCLUSIVE),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.SHARED),
            False,
            id="11",
        ),
        pytest.param(
            double_op,
            (1, 2),
            (LockMode.SHARED, LockMode.SHARED),
            double_op,
            (2, 1),
            (LockMode.SHARED, LockMode.SHARED),
            False,
            id="12",
        ),
        pytest.param(
            triple_op,
            (1, 2, 3),
            (LockMode.EXCLUSIVE, LockMode.SHARED, LockMode.EXCLUSIVE),
            triple_op,
            (3, 1, 2),
            (LockMode.EXCLUSIVE, LockMode.SHARED, LockMode.EXCLUSIVE),
            True,
            id="multi_1",
        ),
        pytest.param(
            triple_op,
            (2, 1, 3),
            (LockMode.SHARED, LockMode.SHARED, LockMode.EXCLUSIVE),
            double_op,
            (3, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE),
            True,
            id="multi_2",
        ),
        pytest.param(
            triple_op,
            (1, 2, 3),
            (LockMode.SHARED, LockMode.EXCLUSIVE, LockMode.SHARED),
            double_op,
            (3, 1, 2),
            (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE, LockMode.SHARED),
            True,
            id="multi_3",
        ),
    ],
)
def test_two_ops(op1, locks1, modes1, op2, locks2, modes2, has_deadlock):
    mgr = LockManager()
    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(op1, mgr, 1, locks1, modes1)
        future2 = executor.submit(op2, mgr, 2, locks2, modes2)
        results = (future1.result(), future2.result())
        assert any(results) == has_deadlock

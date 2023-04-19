import pytest as pytest

from anura.lsm import LSMTree


@pytest.fixture
def my_lsm():
    return LSMTree()


def test_lsm(my_lsm):
    assert my_lsm.get("key") is None
    my_lsm.put("key", "value")
    assert my_lsm.get("key") == "value"
    assert my_lsm.delete("key") == "value"
    assert my_lsm.get("key") is None


def test_delete_lsm(my_lsm):
    assert my_lsm.delete("key") is None

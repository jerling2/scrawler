from typing import NotRequired, Optional, TypedDict
from source import Stock


class AItem(TypedDict):
    a: str

class BItem(TypedDict):
    b: str

class CItem(TypedDict):
    c: Optional[str]

class DItem(TypedDict):
    d: NotRequired[str]

class ABItem(AItem, BItem):
    pass


def test_stock():
    A_VAL = 'item one'
    B_VAL = 'item two'
    C_VAL = 'item three'

    inventory = {
        'a': A_VAL,
        'b': B_VAL,
        'c': C_VAL
    }

    stock = Stock(inventory)

    a_item = stock.collect(AItem)
    b_item = stock.collect(BItem)
    c_item = stock.collect(CItem)
    d_item = stock.collect(DItem)
    ab_item = stock.collect(ABItem)

    assert a_item == {'a': A_VAL}
    assert b_item == {'b': B_VAL}
    assert c_item == {'c': C_VAL}
    assert d_item == {}
    assert ab_item == {
        'a': A_VAL,
        'b': B_VAL
    }
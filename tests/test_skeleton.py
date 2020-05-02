# -*- coding: utf-8 -*-

import pytest
from cusersgem7318documentsgithubsnowmobile.skeleton import fib

__author__ = "Grant E Murray"
__copyright__ = "Grant E Murray"
__license__ = "mit"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)

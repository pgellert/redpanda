# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, Optional, Union


def assert_equal(first: Any, second: Any, msg: Optional[str] = None) -> None:
    """Assert that two values are equal."""
    assert first == second, msg or f"{first!r} != {second!r}"


def assert_not_equal(first: Any,
                     second: Any,
                     msg: Optional[str] = None) -> None:
    """Assert that two values are not equal."""
    assert first != second, msg or f"{first!r} == {second!r} (expected different values)"


def assert_in(member: Any, container: Any, msg: Optional[str] = None) -> None:
    """Assert that a member is found in a container."""
    assert member in container, msg or f"{member!r} not found in {container!r}"


def assert_not_in(member: Any,
                  container: Any,
                  msg: Optional[str] = None) -> None:
    """Assert that a member is not found in a container."""
    assert member not in container, msg or f"{member!r} unexpectedly found in {container!r}"


def assert_true(expr: Any, msg: Optional[str] = None) -> None:
    """Assert that an expression is true."""
    assert expr, msg or f"Expected True, got {expr!r}"


def assert_false(expr: Any, msg: Optional[str] = None) -> None:
    """Assert that an expression is false."""
    assert not expr, msg or f"Expected False, got {expr!r}"


def assert_is(first: Any, second: Any, msg: Optional[str] = None) -> None:
    """Assert that two objects are the same (identity comparison)."""
    assert first is second, msg or f"{first!r} is not {second!r}"


def assert_is_not(first: Any, second: Any, msg: Optional[str] = None) -> None:
    """Assert that two objects are not the same (identity comparison)."""
    assert first is not second, msg or f"{first!r} is {second!r} (expected different objects)"


def assert_is_none(expr: Any, msg: Optional[str] = None) -> None:
    """Assert that an expression is None."""
    assert expr is None, msg or f"Expected None, got {expr!r}"


def assert_is_not_none(expr: Any, msg: Optional[str] = None) -> None:
    """Assert that an expression is not None."""
    assert expr is not None, msg or f"Expected not None, got None"


def assert_isinstance(obj: Any,
                      cls: Union[type, tuple],
                      msg: Optional[str] = None) -> None:
    """Assert that an object is an instance of a given class or classes."""
    assert isinstance(obj, cls), msg or f"{obj!r} is not an instance of {cls}"


def assert_not_isinstance(obj: Any,
                          cls: Union[type, tuple],
                          msg: Optional[str] = None) -> None:
    """Assert that an object is not an instance of a given class or classes."""
    assert not isinstance(obj, cls), msg or f"{obj!r} is an instance of {cls}"


def assert_greater(first: Any, second: Any, msg: Optional[str] = None) -> None:
    """Assert that first > second."""
    assert first > second, msg or f"{first!r} not greater than {second!r}"


def assert_greater_equal(first: Any,
                         second: Any,
                         msg: Optional[str] = None) -> None:
    """Assert that first >= second."""
    assert first >= second, msg or f"{first!r} not greater than or equal to {second!r}"


def assert_less(first: Any, second: Any, msg: Optional[str] = None) -> None:
    """Assert that first < second."""
    assert first < second, msg or f"{first!r} not less than {second!r}"


def assert_less_equal(first: Any,
                      second: Any,
                      msg: Optional[str] = None) -> None:
    """Assert that first <= second."""
    assert first <= second, msg or f"{first!r} not less than or equal to {second!r}"


def assert_empty(container: Any, msg: Optional[str] = None) -> None:
    """Assert that a container is empty."""
    assert len(
        container
    ) == 0, msg or f"Expected empty container, got {container!r} with {len(container)} items"


def assert_not_empty(container: Any, msg: Optional[str] = None) -> None:
    """Assert that a container is not empty."""
    assert len(
        container
    ) > 0, msg or f"Expected non-empty container, got empty {type(container).__name__}"

# pylint: disable=redefined-outer-name
from abc import ABC
from typing import Type

import pytest
from pydantic import Field

from neo4j_app.icij_worker.utils.from_config import C, T
from neo4j_app.icij_worker.utils.registrable import Registrable, RegistrableConfig
from neo4j_app.tests.conftest import fail_if_exception


class _MockedBaseClass(Registrable, ABC):
    pass


@pytest.fixture()
def clear_mocked_registry():
    # pylint: disable=protected-access
    try:
        yield
    finally:
        if _MockedBaseClass in Registrable._registry:
            del Registrable._registry[_MockedBaseClass]


def test_should_register_class(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass
    assert not base_class.list_available()

    # When
    @base_class.register("registered")
    class Registered(base_class):
        @classmethod
        def _from_config(cls: Type[T], config: C, **extras) -> T:
            ...

        def _to_config(self) -> C:
            ...

    # Then
    assert base_class.by_name("registered") is Registered
    available = base_class.list_available()
    assert len(available) == 1
    assert available[0] == "registered"


def test_register_should_raise_for_already_registered(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):  # pylint: disable=unused-variable
        @classmethod
        def _from_config(cls: Type[T], config: C, **extras) -> T:
            ...

        def _to_config(self) -> C:
            ...

    # When
    expected = (
        "Cannot register registered as _MockedBaseClass;"
        " name already in use for Registered"
    )
    with pytest.raises(ValueError, match=expected):

        @base_class.register("registered")
        class Other(base_class):  # pylint: disable=unused-variable
            @classmethod
            def _from_config(cls: Type[T], config: C, **extras) -> T:
                ...

            def _to_config(self) -> C:
                ...


def test_should_register_already_registered_with_exist_ok(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    @base_class.register("registered")
    class Registered(base_class):  # pylint: disable=unused-variable
        @classmethod
        def _from_config(cls: Type[T], config: C, **extras) -> T:
            ...

        def _to_config(self) -> C:
            ...

    # When
    msg = "Failed to register already registered class with exist_ok"
    with fail_if_exception(msg):

        @base_class.register("registered", exist_ok=True)
        class Other(base_class):  # pylint: disable=unused-variable
            @classmethod
            def _from_config(cls: Type[T], config: C, **extras) -> T:
                ...

            def _to_config(self) -> C:
                ...


def test_resolve_class_name_for_fully_qualified_class(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    fully_qualified = "unittest.mock.MagicMock"

    # When
    registered_cls = _MockedBaseClass.resolve_class_name(fully_qualified)

    # Then
    from unittest.mock import MagicMock

    assert registered_cls is MagicMock


def test_registrable_from_config(
    clear_mocked_registry,  # pylint: disable=unused-argument
):
    # Given
    base_class = _MockedBaseClass

    class _MockedBaseClassConfig(RegistrableConfig):
        registry_key: str = Field(const=True, default="some_key")
        some_attr: str
        some_key: str = Field(const=True, default="registered")

    @base_class.register("registered")
    class Registered(base_class):
        def __init__(self, some_attr):
            self._some_attr = some_attr

        @classmethod
        def _from_config(cls: Type[T], config: C, **extras) -> T:
            return cls(some_attr=config.some_attr)

        def _to_config(self) -> C:
            return _MockedBaseClassConfig(some_attr=self._some_attr)

    instance_config = _MockedBaseClassConfig(some_attr="some_value")

    # When
    instance = base_class.from_config(instance_config)

    # Then
    assert isinstance(instance, Registered)

    assert instance.config == instance_config

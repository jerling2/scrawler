from dataclasses import is_dataclass
from typing import TypeVar, TypedDict, get_type_hints


T = TypeVar('T', bound=object)


def as_typed_dict(obj: type[T]) -> type[dict]:
    """
    An very important step this function provides is resolving type hints.

    At first glance, both get_type_hints and __annotations__ functions appear to return the same
    value; a dictionary mapping of attribute to type hint. However, get_type_hints resolves
    ForwardReferences whereas __annotations__ does not. Without resolving the ForwardReferences,
    the location of the true definition is lost when initializing the typeddict class. E.g., python
    would belive that this module <source.utilities.as_typed_dict> contains the typed definitions. 
    The fix is to resolve the ForwardReferences before initializing the TypedDict class.
    """
    if not (is_dataclass(obj) and isinstance(obj, type)):
        raise ValueError('Error: expected an dataclass type')
    resolved_type_hints = get_type_hints(obj)
    return TypedDict(obj.__qualname__, resolved_type_hints)
from typing import TypedDict, TypeVar, is_typeddict
from pydantic import TypeAdapter, ValidationError


GenericTypedDict = TypeVar('GenericTypedDict')


class Stock:

    def __init__(self, inventory: dict={}) -> None:
        self.inventory = self._validate_inventory(inventory)

    def _validate_inventory(self, inventory: dict) -> dict:
         if not isinstance(inventory, dict):
            raise ValueError("Error: expected inventory to be a 'dict' instance")
         return inventory

    def update(self, inventory: dict) -> None:
        self.inventory = self._validate_inventory(inventory)    

    def collect(self, typed_dict: type[GenericTypedDict]) -> GenericTypedDict | None:
        if not is_typeddict(typed_dict):
            raise ValueError("Error: expected typed_dict to be a 'TypedDict' type hint")
        ta = TypeAdapter(typed_dict)
        try:
            ta.validate_python(self.inventory)
        except ValidationError:
            return None
        keys = typed_dict.__annotations__.keys()
        return {k: v for k, v in self.inventory.items() if k in keys}




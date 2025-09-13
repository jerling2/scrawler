"""
Terminal-based user interface module for interactive command-line applications.

Provides Interface base class for building interactive, layered, terminals that
handle input validation, menu navigation, and page flow control. Subclasses
must implement the `interact` method.
"""
from functools import wraps
from typing import Optional
from collections.abc import Callable
from abc import ABC, abstractmethod


class Interface(ABC):
    """Abstract base class for implementing interactive command-line interfaces.

    Methods:
        - format_header: format text as a header.
        - prompt_options: user selects an option out of a list of options.
        - prompt_int: user inputs a valid integer within range.
        - prompt_enter: user inputs an unique name, handles taken names.
        - interact: must be implemented, main entrypoint into the interface.
        
    Decorators:
        @main_page: repeat this method method continuously.
        @sub_page: repeat this method until interrupted by a KeyboardInterrupt.
    """
    @staticmethod
    def main_page(func):
        """Decorator to mark this method as a main page.
        
        Repeatedly executes the decorated function until interrupted.
        Exceptions are propogated to the caller.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            while True:
                func(*args, **kwargs)
        return wrapper

    @staticmethod
    def sub_page(func):
        """Decorator to mark this method as a sub-page.
        
        Repeatedly executes the decorated function until a `KeyboardInterrupt`
        (Ctrl+C | Cmd+C) occurs. When interrupted by a `KeyboardInterrupt`,
        terminate gracefully. Any other exceptions raised by the function are
        propogated to the caller.
        """
        @wraps(func)
        def wrapper(*args, **kwargs) -> None:
            while True:
                try:
                    func(*args, **kwargs)
                except KeyboardInterrupt:
                    # Print a newline because the program was most likely
                    # interrupted while waiting for user-input.
                    return print()
        return wrapper

    @staticmethod
    def format_header(text: str) -> str:
        """Returns a new header string that includes the input text."""
        return f"\x1b[36;1m--  {text} --\x1b[0m"

    def prompt_options(self, header: str, options: list[str], 
                       registery: dict[str, Callable]) -> int:
        """Display a header, numerated list of options, and prompt to the user.

        Display a header (e.g. name of the interface), a numerated list of
        options, and ask the user to select a option by entering an integer.
        Input validation is handled by `prompt_int`. Any option not mapped in
        the registery are written in red with a strikethrough.
        
        Args:
            - header: Header, e.g. the name of the program or page.
            - options: list of options for the user to select from.
            - registery: Map of option to callable function.
        
        Returns:
            - (int): User entered option.
        """
        options_prompt = self.format_header(header) + "\n"
        for i, option in enumerate(options):
            options_prompt += "  "                                #< Add indent
            if option not in registery:
                options_prompt += "\x1b[31;9m"           #< Red + strikethrough
            else:
                options_prompt += "\x1b[36m"            #< Cyan (if registered)
            options_prompt += f"[{i + 1}] {option}\n"
            options_prompt += "\x1b[0m"
        options_prompt += "\x1b[36mPlease select an option: \x1b[0m"
        return self.prompt_int(options_prompt, 1, len(options))

    @staticmethod
    def prompt_int(instructions: str, min_range: Optional[int]=float('-inf'),
                   max_range: Optional[int]=float('inf')) -> int:
        """Prompt the user to enter an integer.
        
        Args:
            - instructions: instructs the user on the expected input.
            - min_range: the minimum allowed integer (inclusive).
            - max_range: the maximum allowed integer (inclusive).

        Returns:
            - (int): User provided integer.
        """
        while True:
            user_input = input(instructions)
            try:
                user_input = int(user_input)
            except (ValueError, TypeError, OverflowError):
                print('\x1b[31mError: Invalid input\x1b[0m')
                continue
            if user_input < min_range or user_input > max_range:
                print('\x1b[31mError: Invalid range\x1b[0m')
                continue
            return user_input

    @staticmethod
    def prompt_enter(instructions: str, taken_names: list[str]) -> tuple[str, bool]:
        """Prompt the user to enter a name.
        
        Args:
            - instructions: instructs the user on the expected input.
            - taken_names: a list of names that are already taken.

        Returns:
            - (str): User provided name.
            - (bool): Whether the user provided name is taken.
        """
        user_input, is_override = None, False
        while not (user_input := input(instructions).strip()):
            print("Invalid name")
        while user_input in taken_names:
            if input("Name is taken. Overwrite [y]? ") == "y":
                is_override = True
                return user_input, is_override
            while not (user_input := input(instructions).strip()):
                print("Invalid name")
        return user_input, is_override

    @abstractmethod
    def interact(self):
        """Main interaction method to be implemented in the subclass.

        Raises:
            TypeError: If called on a class that hasn't implemented this method.
        """

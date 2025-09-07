import sys
import logfire
from dotenv import load_dotenv
from source import System

def main():
    load_dotenv()
    logfire_options = logfire.ConsoleOptions(
        show_project_link=False     #< suppresse the "Logfire project URL: ..." message
    )
    logfire.configure(console=logfire_options)
    logfire.instrument_pydantic_ai()
    program = System()
    try:
        program.interact()
    except KeyboardInterrupt:
        print('\nGoodbye!')
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
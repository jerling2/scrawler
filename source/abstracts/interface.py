class Interface():
    
    @staticmethod
    def format_header(header):
        return f"\x1b[36;1m--  {header} --\x1b[0m"

    def prompt_options(self, header, options, registery):
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
    def prompt_int(message, min_range=float('-inf'), max_range=float('inf')):
        while True:
            user_input = input(message)
            try:
                user_input = int(user_input)
            except:
                print('\x1b[31mError: Invalid input\x1b[0m')
                continue
            if user_input < min_range or user_input > max_range:
                print('\x1b[31mError: Invalid range\x1b[0m')
                continue
            return user_input

    @staticmethod
    def prompt_enter(message, taken_names):
        user_input, is_override = None, False
        while not (user_input := input(message).strip()):
            print("Invalid name")
        while user_input in taken_names:
            if input("Name is taken. Overwrite [y]? ") == "y":
                is_override = True
                return user_input, is_override
            while not (user_input := input(message).strip()):
                print("Invalid name")
        return user_input, is_override

    def interact(self):
        raise NotImplementedError("The 'interact' method must be implemented in concrete subclasses.")
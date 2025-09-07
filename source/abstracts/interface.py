class Interface():
    
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

    def interact(self):
        raise NotImplementedError("The 'interact' method must be implemented in concrete subclasses.")
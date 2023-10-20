class DateParseException(Exception):
    """
    Custom exception for errors eccontered while parsing a date string.
    """
    def __init__(self, e, message="Error occured while parsing log date: "):
        self.message = message + str(e)
        super().__init__(message)

    def __str__(self):

        return self.message

class SplitStringException(Exception):
    """
    Custom exception for errors eccontered while splitting a string.
    """
    def __init__(self, e, message="Error occurred while splitting: "):
        self.message = message + str(e)
        super().__init__(self.message)

    def __str__(self):
        return self.message
    

class NotFoundRoundingChars(Exception):
    """
    Custom exception for errors eccountered when rounding chars in date are not found
    """
    def __init__(self, e, message="Error occurred while finding the date: "):
        self.message = message + str(e)
        super().__init__(self.message)

    def __str__(self):
        return self.message



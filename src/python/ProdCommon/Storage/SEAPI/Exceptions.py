

class NotExistsException(Exception):
    """
    errors with not existing path
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class TransferException(Exception):
    """
    generic transfer error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class ProtocolOperationException(Exception):
    """
    generic exception for an operation error
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


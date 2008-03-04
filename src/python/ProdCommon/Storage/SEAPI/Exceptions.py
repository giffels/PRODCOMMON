class ProtocolException(Exception):
    """
    general super
    """

    def __init__(self, value, detail = [], out = ""):
#        super(ProtocolException, self).__init__()
        self.value = value
        self.detail = detail
        self.output = out

    def __str__(self):
        return repr(self.value)


class NotExistsException(ProtocolException):
    """
    errors with not existing path
    """
    pass

class TransferException(ProtocolException):
    """
    generic transfer error
    """
    pass

class OperationException(ProtocolException):
    """
    generic exception for an operation error
    """
    pass

class ProtocolMismatch(ProtocolException):
    """
    exception for mismatch between protocols
    """
    pass

class ProtocolUnknown(ProtocolException):
    """
    error for an unknown protocol
    """
    pass

class MissingDestination(ProtocolException):
    """
    error for missing destination
    """
    pass


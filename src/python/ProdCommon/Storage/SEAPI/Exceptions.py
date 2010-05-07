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

class WrongOption(ProtocolException):
    """
    when a command fails for a wrong option
    """
    pass

class AlreadyExistsException(ProtocolException):
    """
    when trying to create or copy over an already existsing file
    (depends on the protocol used)
    """
    pass

class AuthorizationException(ProtocolException):
    """
    authrization/permission problem when executing a command
    """
    pass

class SizeZeroException(ProtocolException):
    """
    actually used when a file to copy has zero size
    (check before transfer)
    """
    pass

class MissingCommand(ProtocolException):
    """
    when a command is not found
    """
    pass

class NFSException(ProtocolException):
    """
    'Stale NFS file handle'
    """
    pass

class SEAPITimeout(ProtocolException):
    """
    when subprocess is killed for timeout limit 
    """
    pass

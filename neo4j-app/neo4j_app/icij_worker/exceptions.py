import abc


class ICIJWorkerError(metaclass=abc.ABCMeta):
    ...


class MaxReconnectionExceeded(ICIJWorkerError, ConnectionError):
    ...


class ConnectionLostError(ConnectionError):
    ...

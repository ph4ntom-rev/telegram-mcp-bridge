"""Domain errors intentionally free of credentials and Telegram request URLs."""


class BridgeError(Exception):
    """Base error safe to expose to an MCP client."""


class ConfigurationError(BridgeError):
    """The operator configuration is invalid or unsafe."""


class AuthorizationError(BridgeError):
    """An inbound actor or outbound target is not allowed."""


class QueueFullError(BridgeError):
    """The durable inbox reached its configured hard limit."""


class IdempotencyConflictError(BridgeError):
    """An idempotency key was reused with different logical input."""


class RuntimeNotReadyError(BridgeError):
    """The bridge runtime has not finished starting or is shutting down."""

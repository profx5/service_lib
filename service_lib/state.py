from typing import Any, Dict, Optional


class State:
    """Контейнер стейта приложения"""

    def __init__(self, state: Optional[Dict[str, Any]] = None) -> None:
        if state is None:
            state = {}
        super(State, self).__setattr__("_state", state)

    def __setattr__(self, key: str, value: Any) -> None:
        self._state[key] = value

    def __getattr__(self, key: str) -> Any:
        try:
            return self._state[key]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __delattr__(self, key: str) -> None:
        del self._state[key]

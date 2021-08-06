from typing import Optional, Sequence, TypeVar

T = TypeVar("T")


def safe_get_by_idx(target_list: Sequence[T], idx: int) -> Optional[T]:
    try:
        return target_list[idx]
    except IndexError:
        return None

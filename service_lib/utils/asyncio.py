import asyncio
from datetime import datetime


async def wait_until(dt: datetime) -> None:
    now = datetime.utcnow()
    await asyncio.sleep((dt - now).total_seconds())

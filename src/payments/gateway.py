import asyncio
import hashlib
import random
from uuid import UUID

from payments.config import Settings


class SimulatedGateway:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def charge(self, payment_id: UUID) -> str:
        await asyncio.sleep(
            random.uniform(self.settings.gateway_delay_min, self.settings.gateway_delay_max)
        )
        # An idempotent external gateway returns the same outcome for the same payment ID.
        draw = int.from_bytes(hashlib.sha256(payment_id.bytes).digest()[:8], "big") / 2**64
        return "succeeded" if draw < self.settings.gateway_success_rate else "failed"

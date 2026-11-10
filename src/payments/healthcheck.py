"""Authenticated local probe that never forwards API credentials to an HTTP proxy."""

import os
from urllib.request import ProxyHandler, Request, build_opener


def check(url: str) -> None:
    request = Request(url, headers={"X-API-Key": os.environ["API_KEY"]})
    with build_opener(ProxyHandler({})).open(request, timeout=3) as response:
        if response.status != 200:
            raise RuntimeError("API health probe failed")


if __name__ == "__main__":
    check("http://127.0.0.1:8000/health")

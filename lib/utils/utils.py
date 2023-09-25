import requests
from typing import List
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential
from typing import Optional, List


# Network IO
@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=1, max=5))
def http_request(
        method: str,
        url: str,
        data: Optional[dict] = None,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        skip: Optional[List[int]] = None,
):

    r = requests.request(
        method=method,
        url=url,
        data=data,
        json=json,
        params=params,
        headers=headers,
        timeout=timeout,
    )

    if skip and r.status_code in skip:
        return r

    if r.status_code >= 400:
        print(
            f"Failed to {method.upper()} -> '{url}', status code: {r.status_code}, error: {r.text}"
        )

    r.raise_for_status()

    print(
        f"Success {method.upper()} -> '{url}' with status code: {r.status_code}"
    )

    return r

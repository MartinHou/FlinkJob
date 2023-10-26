import requests
from typing  import Optional, List
ARS_HOST = 'https://ars.momenta.works'
from tqdm import tqdm
ARS_API_ROOT_TOKEN = '8e4c872d-b688-4900-83b1-b28a8efd4001'


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

l = []
with open('/home/martin.hou/test.txt', 'rb') as f:
    for line in f:
        print(len(line.decode().strip().split('-')))
        print(line.decode().strip().split('-'))
        exit(0)
# for workflow in tqdm(l):
#     http_request(
#         method='PUT',
#         url=ARS_HOST +
#         '/api/v1/driver/workflow/improve_priority/3',  # TODO: change to ARS_HOST
#         data={'workflow_id': workflow},
#         headers={
#             'Authorization': 'Token ' + ARS_API_ROOT_TOKEN
#         })
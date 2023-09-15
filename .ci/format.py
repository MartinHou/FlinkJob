#!/usr/bin/env python
"""
Code formate tools.

Make sure you have install yapf and its version is 0.25.0.

```bash
python3 -m pip install yapf==0.25.0
```

You can check whether code is formatted through this:
```bash
python3 .ci/format.py check
```

Make sure your code is formatted before pull merge request to dev branch.
```bash
python3 .ci/format.py
```
"""

import os
import subprocess
import argparse


def error(msg=''):
    print('error: %s' % msg)
    exit(1)


def get_root_path(anchor='manage.py'):
    path = os.path.abspath(__file__)
    while True:
        path = os.path.dirname(path)
        if os.path.exists(path + '/' + anchor):
            return path
        if path == '/':
            error('%s not found' % anchor)


# python
def run(action):
    format_file = '.ci'
    root = get_root_path(format_file)
    print(root)
    file_types = ['*.py']
    sub_folders = [
        'ars',
    ]

    print('format in [%s] with [%s]' % (', '.join(sub_folders),
                                        ', '.join(file_types)))
    for folder in sub_folders:
        for file_type in file_types:
            if action == 'run':
                print('format in [%s] with [%s]' % (folder, file_type))
                try:
                    cmd = 'find %s/%s -name \'%s\' | xargs yapf -i' % (
                        root, folder, file_type)
                    os.system(cmd)
                except Exception as e:
                    print(e)
            else:
                try:
                    cmd = 'yapf -d -r %s/%s \'%s\'' % (root, folder, file_type)
                    subprocess.check_output(cmd, shell=True)
                except subprocess.CalledProcessError:
                    error('not all %s in %s/%s is formatted' % (file_type,
                                                                root, folder))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'action',
        choices=['check', 'run'],
        nargs='?',
        default='run',
        help='The actions')
    args = parser.parse_args()
    run(args.action)

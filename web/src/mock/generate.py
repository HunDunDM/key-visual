#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
#
# Copyright (c) 2019 HunDunDM. All Rights Reserved
#
"""

generate mock data

@file: generate.py
@Authors: HunDunDM (hundundm@gmail.com)
@Date: 2019/10/25
"""

import json
import random

def rand_key_bytes(n):
    key = random.randint(0, n)
    bytes = key + random.randint(0, n)
    return key, bytes

def generate_region_value():
    read_keys, read_bytes = rand_key_bytes(1000000)
    written_keys, written_bytes = rand_key_bytes(100000)
    return {
        'read_keys': read_keys,
        'read_bytes': read_bytes,
        'written_keys': written_keys,
        'written_bytes': written_bytes,
    }

def generate_stat_unit():
    return {
        'max': generate_region_value(),
        'average': generate_region_value(),
    }

def generate_keys(n):
    keys = []
    for i in range(n+1):
        keys.append(str(i))
    return keys

def generate_times(m):
    return [
        "2019-10-25T22:45:47.9344274+08:00",
        "2019-10-25T22:46:48.9337329+08:00",
        "2019-10-25T22:47:49.9240867+08:00",
        "2019-10-25T22:48:50.9353787+08:00",
        "2019-10-25T22:49:51.9307719+08:00",
        "2019-10-25T22:50:52.9402905+08:00",
        "2019-10-25T22:51:53.9631405+08:00",
        "2019-10-25T22:52:54.9436482+08:00",
        "2019-10-25T22:53:55.9300509+08:00",
        "2019-10-25T22:54:56.9323304+08:00",
        "2019-10-25T22:55:57.9356675+08:00",
        "2019-10-25T22:56:58.9269951+08:00",
        "2019-10-25T22:57:59.923645+08:00",
        "2019-10-25T22:58:00.9249693+08:00",
        "2019-10-25T22:59:01.9224322+08:00",
        "2019-10-25T23:00:57.9356675+08:00",
        "2019-10-25T23:01:58.9269951+08:00",
        "2019-10-25T23:02:59.923645+08:00",
        "2019-10-25T23:03:00.9249693+08:00",
        "2019-10-25T23:04:01.9224322+08:00",
        "2019-10-25T23:05:01.9224322+08:00",
    ]

def generate(n, m):
    data = []
    for i in range(n):
        line = []
        for j in range(m):
            line.append(generate_stat_unit())
        data.append(line)
    with open('mock.json', 'w') as ofs:
        json.dump({ 'data': data, 'keys': generate_keys(n), 'times': generate_times(m) }, ofs, indent=2)

if __name__ == '__main__':
    generate(20, 40)

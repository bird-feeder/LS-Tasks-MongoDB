#!/usr/bin/env python
# coding: utf-8

import os
import signal
import sys
import time

import ray
import requests
from dotenv import load_dotenv
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

from mongodb_helper import get_tasks_from_mongodb, mongodb_db


def keyboard_interrupt_handler(sig, _):
    """This function handles the KeyboardInterrupt (CTRL+C) signal.
    It's a handler for the signal, which means it's called when the OS sends the signal.
    The signal is sent when the user presses CTRL+C.

    Parameters
    ----------
    The function takes two arguments:
    sig:
        The ID of the signal that was sent.
    frame:
        The current stack frame.
    """
    print(f'KeyboardInterrupt (ID: {sig}) has been caught...')
    ray.shutdown()
    print('Terminating the session gracefully...')
    sys.exit(1)


@ray.remote
def img_url_to_binary(x):
    return {
        '_id': x['_id'],
        'file_name': x['data']['_image'].replace('https://srv.aibird.me/', ''),
        'image': requests.get(x['data']['_image']).content
    }


def insert_image(d):
    try:
        db.images.insert_one(d)
    except DuplicateKeyError:
        db.images.delete_one({'_id': d['_id']})
        db.images.insert_one(d)


def main():
    existing_ids = db.images.find().distinct('_id')
    projects_id = os.environ['PROJECTS_ID'].split(',')
    data = sum([
        get_tasks_from_mongodb(project_id, dump=False, json_min=False)
        for project_id in projects_id
    ], [])

    data = [x for x in data if x['_id'] not in existing_ids]

    futures = []
    for x in data:
        futures.append(img_url_to_binary.remote(x))

    results = []
    for future in tqdm(futures):
        insert_image(ray.get(future))


if __name__ == '__main__':
    load_dotenv()
    signal.signal(signal.SIGINT, keyboard_interrupt_handler)
    db = mongodb_db()
    main()  # run once before schedule
    schedule.every(6).hours.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

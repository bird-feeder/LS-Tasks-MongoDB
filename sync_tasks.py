#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import sys
import time

import pymongo
import requests
import schedule
from dotenv import load_dotenv
from loguru import logger


def to_srv(url):
    return url.replace(f'{os.environ["LS_HOST"]}/data/local-files/?d=',
                       f'{os.environ["SRV_HOST"]}/')


def api_request(url):
    headers = requests.structures.CaseInsensitiveDict()
    headers['Authorization'] = f'Token {os.environ["TOKEN"]}'
    resp = requests.get(url, headers=headers)
    return resp.json()


def mongodb_db():
    client = pymongo.MongoClient(os.environ['DB_CONNECTION_STRING'])
    db = client[os.environ['DB_NAME']]
    return db


def run(project_id, json_min=False):
    """This function is used to update the database with the latest data from
    the server.
    It takes the project id as an argument and then makes an API request to
    the server to get the number of tasks and annotations in the project.
    It then connects to the database and gets the number of tasks and
    annotations in the database.
    If the number of tasks and annotations in the database is not equal to
    the number of tasks and annotations in the server, then it makes another
    API request to get the data of all the tasks in the project.
    It then updates the database with the latest data.
    """
    project_data = api_request(
        f'{os.environ["LS_HOST"]}/api/projects/{project_id}/')

    tasks_len_ls = project_data['task_number']
    if tasks_len_ls == 0:
        logger.warning(f'No tasks in project {project_id}! Skipping...')
        return
    anno_len_ls = project_data['num_tasks_with_annotations']
    ls_lens = (tasks_len_ls, anno_len_ls)
    logger.debug(f'Project {project_id}:\n'
                 f'Tasks: {tasks_len_ls}\nAnnotations: {anno_len_ls}')

    db = mongodb_db()
    if json_min:
        col = db[f'project_{project_id}_min']
    else:
        col = db[f'project_{project_id}']
    tasks_len_mdb = len(list(col.find({}, {})))
    anno_len_mdb = len(list(col.find({"annotations": {'$ne': []}}, {})))
    mdb_lens = (tasks_len_mdb, anno_len_mdb)

    if (not args.json_min
            and ls_lens != mdb_lens) or (args.json_min
                                         and anno_len_ls != anno_len_mdb):
        _msg = lambda x: f'Difference in {x} number'
        logger.debug(f'Project {project_id} has changed. Updating...\n'
                     f'{_msg("tasks")}: {tasks_len_ls - tasks_len_mdb}\n'
                     f'{_msg("annotations")}: {anno_len_ls - anno_len_mdb}')

        if json_min:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON_MIN&download_all_tasks=true')
        else:
            data = api_request(
                f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
                '?exportType=JSON&download_all_tasks=true')

        for task in data:
            if json_min:
                img = task['image']
            else:
                img = task['data']['image']
            task.update({
                '_id': task['id'],
                'data': {
                    '_image': to_srv(img),
                    'image': img
                }
            })

        col.drop()
        col.insert_many(data)

    else:
        logger.debug(f'No changes were detected in project {project_id}...')


def opts():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--projects',
                        help='Comma-seperated projects ID',
                        type=str,
                        default=os.environ['PROJECTS_ID'])
    parser.add_argument('-o',
                        '--once',
                        help='Run once and exit',
                        action='store_true')
    parser.add_argument('-m',
                        '--json-min',
                        help='Export as JSON_MIN',
                        action='store_true')
    return parser.parse_args()


def main():
    projects_id = args.projects.split(',')
    for project_id in projects_id:
        run(project_id, args.json_min)
        logger.info(f'Finished processing project {project_id}')


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')
    args = opts()

    if args.once:
        main()
        sys.exit(0)
    schedule.every(10).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

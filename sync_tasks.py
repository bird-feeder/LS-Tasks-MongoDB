import os
import sys
import time

import requests
import pymongo
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


def run(project_id):
    project_data = api_request(
        f'{os.environ["LS_HOST"]}/api/projects/{project_id}/')
    tasks_len_ls = project_data['task_number']
    anno_len_ls = project_data['num_tasks_with_annotations']
    ls_lens = (tasks_len_ls, anno_len_ls)
    logger.debug(f'Project {project_id}:\n'
                 f'Tasks: {tasks_len_ls}\nAnnotations: {anno_len_ls}')

    db = mongodb_db()
    col = db[f'project_{project_id}']
    tasks_len_mdb = len(list(col.find({}, {})))
    anno_len_mdb = len(list(col.find({"annotations": {'$ne': []}}, {})))
    mdb_lens = (tasks_len_mdb, anno_len_mdb)

    if ls_lens != mdb_lens:
        _msg = lambda x: f'Difference in {x} number'
        logger.debug(
            f'Project {project_id} has changed. Updating...\n'
            f'{_msg("tasks")}: {tasks_len_ls - tasks_len_mdb}\n'
            f'{_msg("annotations")}: {anno_len_ls - anno_len_mdb}')

        data = api_request(
            f'{os.environ["LS_HOST"]}/api/projects/{project_id}/export'
            '?exportType=JSON&download_all_tasks=true')

        for task in data:
            task.update({
                '_id': task['id'],
                'data': {
                    '_image': to_srv(task['data']['image']),
                    'image': task['data']['image']
                }
            })

        col.drop()
        col.insert_many(data)

    else:
        logger.debug(f'No changes were detected in project {project_id}...')


def main():
    projects_id = os.environ['PROJECTS_ID'].split(',')
    for project_id in projects_id:
        run(project_id)
        logger.info(f'Finished processing project {project_id}')


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')

    if '--once' in sys.argv:
        main()
        sys.exit(0)
    schedule.every(10).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

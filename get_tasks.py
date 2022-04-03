import os
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
    logger.debug(f'Number of tasks in project {project_id}: {tasks_len_ls}')

    db = mongodb_db()
    col = db[f'project_{project_id}']
    tasks_len_mdb = len(list(col.find({}, {})))
    anno_len_mdb = len(list(col.find({"annotations": {'$ne': []}}, {})))

    if tasks_len_ls == tasks_len_mdb:
        logger.debug(
            f'The number of tasks in project {project_id} has not changed.')
    else:
        logger.debug(
            f'The number of tasks in project {project_id} has changed! '
            f'Found {tasks_len_ls - tasks_len_mdb} new tasks.'
        )

    if anno_len_ls == anno_len_mdb:
        logger.debug(f'The number of annotations in project {project_id} '
                     'has not changed.')
    else:
        logger.debug(
            f'The number of annotations in in project {project_id} '
            f'has changed! Found {anno_len_ls - anno_len_mdb} new annotations.'
        )

    if anno_len_ls != anno_len_mdb or tasks_len_mdb == 0:
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


def main():
    for project_id in [1, 10]:
        run(project_id)
        logger.info(f'Finished processing project {project_id}')


if __name__ == '__main__':
    load_dotenv()
    logger.add('logs.log')
    schedule.every(10).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

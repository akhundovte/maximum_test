import os
import csv
import time
import uuid
import pytz
import logging
import requests

from datetime import datetime
from collections import deque


TOKEN_AUTH = 'bb3bb5cc-406c-4005-872f-abc83a2757a6'
FILENAME = 'results.csv'
PATH_PROJ = os.path.dirname(os.path.abspath(__file__))

url_api = 'https://analytics.maximum-auto.ru/vacancy-test/api/v0.1'
tmpl_url_report = url_api + '/reports/{report_id}'
tzlocal = pytz.timezone('Europe/Moscow')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_report():
    report_id = uuid.uuid4().hex
    url = tmpl_url_report.format(report_id=report_id)
    headers = {'Authorization': _get_auth_header(TOKEN_AUTH)}
    try:
        response = requests.put(url, headers=headers)
    except requests.RequestException as e:
        raise ServiceError(str(e))

    if response.status_code == 201:
        return report_id
    elif response.status_code == 409:
        raise ReportAlreadyExist(
            f"Отчет (id {report_id}) уже существует")
    else:
        raise ServiceError(response.text)


def get_report(report_id):
    url = tmpl_url_report.format(report_id=report_id)
    headers = {'Authorization': _get_auth_header(TOKEN_AUTH)}
    try:
        response = requests.get(url, headers=headers)
    except requests.RequestException as e:
        raise ServiceError(str(e))

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 202:
        raise ReportNotReady
    elif response.status_code == 404:
        raise ReportDoesNotExist(
            f"Отчет (id {report_id}) не существует")
    else:
        raise ServiceError(response.text)


def _get_auth_header(token):
    return f"Bearer {token}"


def _write_row_to_csv(timestamp, value):
    path = os.path.join(PATH_PROJ, FILENAME)
    with open(path, 'a', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=';')
        spamwriter.writerow([timestamp, value])


def _dt_now_iso():
    return datetime.now(tzlocal).isoformat()


def task_create_report(reports_queue):
    try:
        report_id = create_report()
        reports_queue.append(report_id)
        logger.info(f"{_dt_now_iso()} CREATE {report_id}")
    except ReportAlreadyExist as e:
        logger.warning(str(e))


def task_get_report(reports_queue):
    if len(reports_queue) == 0:
        return

    report_id = reports_queue.popleft()
    try:
        report_data = get_report(report_id)
        _write_row_to_csv(time.time(), report_data['value'])
        logger.info(f"{_dt_now_iso()} SAVE {report_id}")
    except ReportNotReady:
        reports_queue.append(report_id)
        logger.info(f"{_dt_now_iso()} WAIT {report_id}")
    except ReportDoesNotExist as e:
        logger.warning(str(e))


def run_schedule():
    reports_queue = deque()
    try:
        count = 0
        while True:
            if count == 0:
                try:
                    task_create_report(reports_queue)
                    count = 60
                except ServiceError as e:
                    logger.error(str(e))
                    break

            try:
                task_get_report(reports_queue)
            except ServiceError as e:
                logger.error(str(e))
                break

            time.sleep(1)
            count -= 1
    except KeyboardInterrupt:
        return


class ServiceError(Exception):
    pass


class ReportNotReady(ServiceError):
    pass


class ReportAlreadyExist(ServiceError):
    pass


class ReportDoesNotExist(ServiceError):
    pass


if __name__ == "__main__":
    run_schedule()

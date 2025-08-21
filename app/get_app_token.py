#!/usr/bin/python3
import logging
import requests
import hashlib
import argparse

__author__ = "Kosterev Grigoriy <kosterev@starline.ru>"
__date__ = "13.10.2018"


def get_app_token(app_id, app_secret, app_code):
    """
    Получение токена приложения для дальнейшей авторизации.
    Время жизни токена приложения - 4 часа.
    Идентификатор приложения и пароль можно получить на my.starline.ru.
    :param app_id: Идентификатор приложения
    :param app_secret: Пароль приложения
    :param app_code: Код приложения
    :return: Токен приложения
    """
    url = 'https://id.starline.ru/apiV3/application/getToken/'
    logging.info('execute request: {}'.format(url))
    payload = {
        'appId': app_id,
        'secret': hashlib.md5((app_secret + app_code).encode('utf-8')).hexdigest()
    }
    r = requests.get(url, params=payload)
    response = r.json()
    logging.info('payload: {}'.format(payload))
    logging.info('response info: {}'.format(r))
    logging.info('response data: {}'.format(response))
    if int(response['state']) == 1:
        app_token = response['desc']['token']
        logging.info('Application token: {}'.format(app_token))
        return app_token
    raise Exception(response)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--appId", dest="appId", help="application identifier", default="", required=True)
    parser.add_argument("-s", "--appSecret", dest="appSecret", help="application secret", default="", required=True)
    parser.add_argument("-c", "--appCode", dest="appCode", help="application code", default="", required=True)
    args = parser.parse_args()
    logging.info('appId: {}, appSecret: {}, appCode: {}'.format(args.appId, args.appSecret, args.appCode))
    return args


def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    args = get_args()
    get_app_token(args.appId, args.appSecret, args.appCode)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(e)
#get_app_code.py
#!/usr/bin/python3
import logging
import requests
import hashlib
import argparse

__author__ = "Kosterev Grigoriy <kosterev@starline.ru>"
__date__ = "13.10.2018"


def get_app_code(app_id, app_secret):
    """
    Получение кода приложения для дальнейшего получения токена.
    Идентификатор приложения и пароль выдаются контактным лицом СтарЛайн.
    Срок годности кода приложения 1 час.
    :param app_id: Идентификатор приложения
    :param app_secret: Пароль приложения
    :return: Код, необходимый для получения токена приложения
    """
    url = 'https://id.starline.ru/apiV3/application/getCode/'
    logging.info('execute request: {}'.format(url))

    payload = {
        'appId': app_id,
        'secret': hashlib.md5(app_secret.encode('utf-8')).hexdigest()
    }
    r = requests.get(url, params=payload)
    response = r.json()
    logging.info('payload : {}'.format(payload))
    logging.info('response info: {}'.format(r))
    logging.info('response data: {}'.format(response))
    if int(response['state']) == 1:
        app_code = response['desc']['code']
        logging.info('Application code: {}'.format(app_code))
        return app_code
    raise Exception(response)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--appId", dest="appId", help="application identifier", default="", required=True)
    parser.add_argument("-s", "--appSecret", dest="appSecret", help="account secret", default="", required=True)
    args = parser.parse_args()
    logging.info('appId: {}, appSecret: {}'.format(args.appId, args.appSecret))
    return args


def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    args = get_args()
    get_app_code(args.appId, args.appSecret)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(e)
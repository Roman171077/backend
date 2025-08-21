#!/usr/bin/python3
import logging
import requests
import argparse


__author__ = "Kosterev Grigoriy <kosterev@starline.ru>"
__date__ = "13.10.2018"


def get_slnet_token(slid_token):
    """
    Авторизация пользователя по токену StarLineID. Токен авторизации предварительно необходимо получить на сервере StarLineID.
    :param slid_token: Токен StarLineID
    :return: Токен пользователя на StarLineAPI
    """
    url = 'https://developer.starline.ru/json/v2/auth.slid'
    logging.info('execute request: {}'.format(url))
    data = {
        'slid_token': slid_token
    }
    r = requests.post(url, json=data)
    response = r.json()
    logging.info('response info: {}'.format(r))
    logging.info('response data: {}'.format(response))
    slnet_token = r.cookies["slnet"]
    logging.info('slnet token: {}'.format(slnet_token))
    return slnet_token


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--slid_token", dest="slidToken", help="StarLineID Token", default="", required=True)
    args = parser.parse_args()
    logging.info("slidToken: {}".format(args.slidToken))
    return args


def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    args = get_args()
    get_slnet_token(args.slidToken)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(e)
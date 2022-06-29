import os
import sys

import logbook


def _path(path, *args):
    p = os.path.join(path, *args) if len(args) > 0 else path
    if not os.path.exists(os.path.dirname(p)):
        try:
            os.makedirs(os.path.dirname(p))
        except:
            return None
    return p


def _user_path(name, *args):
    path = os.path.expanduser('~')
    default_quant_path = os.path.join(path, '.quant')
    path = os.getenv("QUANT_PATH", default_quant_path)
    data = os.path.join(path, name)
    return _path(data, *args)


def info_path(*args):
    return _user_path("info", *args)


def data_path(*args):
    return _user_path("data", *args)


def cache_path(*args):
    return _user_path("cache", *args)


def _local_path(name, *args):
    path = os.path.dirname(os.path.dirname(__file__))
    data = os.path.join(path, '.quant', name)
    return _path(data, *args)


def local_data_path(*args):
    return _local_path('data', *args)


def gen_log(name=""):
    logname = data_path('TEST-' + name + '.log')

    # if os.path.exists(logname):
    #     os.rename(logname, logname + "~")
    logger = logbook.Logger(name)
    if logname:
        logger.handlers.append(logbook.FileHandler(logname, level='DEBUG', bubble=True))
    logger.handlers.append(logbook.StreamHandler(sys.stdout, level='DEBUG', bubble=True))
    logger.warn("Start of logging")
    return logger


log = gen_log("default")

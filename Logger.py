import logging

class Logger:

    def __init__(self, logger_name):
        log = logging.getLogger(logger_name)
        log.setLevel(logging.INFO)

        log = logging.getLogger(logger_name)
        log.setLevel(logging.INFO)

        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler('StatsPredictor.log')
        c_handler.setLevel(logging.NOTSET)
        f_handler.setLevel(logging.NOTSET)

        c_format = logging.Formatter(
            '%(message)s - FILE: %(filename)s - FUNC: %(funcName)s - LINE: %(lineno)d')
        f_format = logging.Formatter(
            '%(asctime)s - MESSAGE: %(message)s - %(levelname)s - FILE: %(filename)s - FUNC: %(funcName)s - LINE: %(lineno)d')

        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        log.addHandler(c_handler)
        log.addHandler(f_handler)

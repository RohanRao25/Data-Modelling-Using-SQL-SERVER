import logging

class LoggingHelper:
    _instance = None

    def __new__(cls, filename):
        if cls._instance is None:
            cls._instance = super(LoggingHelper, cls).__new__(cls)
            cls._instance.init_logger(filename)
        return cls._instance

    def init_logger(self, filename):
        self.filename = filename
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        file_handler = logging.FileHandler(self.filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message,exc_info=True)

    def exception(self, message):
        self.logger.error(message,exc_info=True)

# Create a single instance of the CustomLogger class
logger = LoggingHelper('pipeline.log')

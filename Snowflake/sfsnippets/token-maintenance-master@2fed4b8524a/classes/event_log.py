import logging
import datetime
import pathlib
import os
import sys

class eventLog:
    def __init__(self, config, jobName):
        self.config = config
        now = datetime.datetime.now()

        jobName = jobName.strip()
        if(jobName == ""):
            jobName = now.strftime("%H%M%S_") + str(os.getpid())
            logName = jobName + ".log"
        else:
            logName = jobName + now.strftime("_%H%M%S_") + str(os.getpid()) + ".log"

        currentDate = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
       # print(self.config.defaultlogfilepath )
        logDir = self.config.defaultlogfilepath + r"/" + currentDate
        pathlib.Path(logDir).mkdir(parents=True, exist_ok=True)

        logFile = os.path.join(logDir, logName)

#        formatString = "[%(asctime)s - %(filename)s:%(lineno)d - %(funcName)s()] %(levelname)s - %(message)s"
        formatString = "[%(asctime)s - %(levelname)s] %(message)s"
        timeFormat = "%Y-%m-%d %H:%M:%S"

        formatter = logging.Formatter(formatString, datefmt = timeFormat)

        fileHandler = logging.FileHandler(logFile)
        streamHandler = logging.StreamHandler(sys.stdout)
        self.logger = logging.getLogger(jobName + ".logger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

        self.fileLogger = logging.getLogger(jobName + ".fileLogger")
        self.fileLogger.setLevel(logging.INFO)
        self.fileLogger.addHandler(fileHandler)

        self.streamLogger = logging.getLogger(jobName + ".streamLogger")
        self.streamLogger.setLevel(logging.INFO)
        self.streamLogger.addHandler(streamHandler)

        fileHandler = logging.FileHandler(logFile)
        streamHandler = logging.StreamHandler(sys.stdout)
        fileHandler.setFormatter(formatter)
        streamHandler.setFormatter(formatter)
        self.formattedLogger = logging.getLogger(jobName + ".formattedLogger")
        self.formattedLogger.setLevel(logging.INFO)
        self.formattedLogger.addHandler(fileHandler)
        self.formattedLogger.addHandler(streamHandler)

        self.formattedFileLogger = logging.getLogger(jobName + ".formattedFileLogger")
        self.formattedFileLogger.setLevel(logging.INFO)
        self.formattedFileLogger.addHandler(fileHandler)

        self.formattedStreamLogger = logging.getLogger(jobName + ".formattedStreamLogger")
        self.formattedStreamLogger.setLevel(logging.INFO)
        self.formattedStreamLogger.addHandler(streamHandler)

    def logInfo(self, message, customFormat = True):
        if(customFormat):
            self.formattedLogger.info(message)
        else:
            self.logger.info(message)

    def logWarn(self, message, customFormat = True):
        if(customFormat):
            self.formattedLogger.warn(message)
        else:
            self.logger.warn(message)

    def logError(self, message, customFormat = True):
        if(customFormat):
            self.formattedLogger.error(message)
        else:
            self.logger.error(message)

    def logMessage(self, type, message, customFormat = True):
        if(type == "INFO"):
            self.logInfo(message, customFormat)
        elif(type == "ERROR"):
            self.logError(message, customFormat)
        else:
            self.logWarn(message, customFormat)


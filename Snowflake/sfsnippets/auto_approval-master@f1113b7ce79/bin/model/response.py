class Response:
    u""" Response body object storing model response, model components, status code, and return message"""

    def __init__(self, response, model, status, message, error):
        self.__response = response
        self.__model = model
        self.__status = status
        self.__message = message
        self.__error = error

    def __repr__(self):
        return '{}, {}, {}, {}'.format(self.__response,
                                       self.__model,
                                       self.__status,
                                       self.__message,
                                       self.__error)

    def __str__(self):
        return str({'RESPONSE': self.__response,
                    'MODEL': self.__model,
                    'STATUS': self.__status,
                    'MESSAGE': self.__message,
                    'ERROR': self.__error})

    @property
    def response(self):
        return self.__response

    @response.setter
    def response(self, response):
        self.__response = response

    @property
    def model(self):
        return self.__model

    @model.setter
    def model(self, model):
        self.__model = model

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, status):
        self.__status = status

    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, message):
        self.__message = message

    @property
    def error(self):
        return self.__error

    @error.setter
    def error(self, error):
        self.__error = error

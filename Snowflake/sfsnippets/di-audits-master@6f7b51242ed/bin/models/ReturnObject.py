# DEFUNCT CLASS
#: TODO: remove


class ReturnObject(object):
    def __init__(self, STATUS_CODE, ERROR, MESSAGE, RETURN_OBJECT_TYPE, RETURN_OBJECT):
        self.status_code = STATUS_CODE
        self.error = ERROR
        self.message = MESSAGE
        self.return_object_type = RETURN_OBJECT_TYPE
        self.return_object = RETURN_OBJECT

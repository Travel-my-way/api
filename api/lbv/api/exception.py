class ApiException(Exception):
    def __init__(self, message, request_id=None):
        self.request_id = request_id
        self.message = message

        super(ApiException, self).__init__(self.message)

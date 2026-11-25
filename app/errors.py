"""应用层错误类型。"""


class ValidationError(Exception):
    """请求数据不合法，对应 HTTP 400。"""

    status = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class NotFoundError(Exception):
    """目标记录不存在，对应 HTTP 404。"""

    status = 404

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

"""Some missing functions from pypika"""
from pypika.functions import Function


class Greatest(Function):
    def __init__(self, *args, alias=None):
        super().__init__('GREATEST', *args, alias=alias)


class DateTrunc(Function):
    def __init__(self, *args, alias=None):
        super().__init__('DATE_TRUNC', *args, alias=alias)


class DatePart(Function):
    def __init__(self, part, expr):
        super(DatePart, self).__init__('DATE_PART', part, expr)

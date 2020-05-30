"""Some missing functions from pypika"""
from pypika.functions import Function


class Greatest(Function):
    def __init__(self, *args, alias=None):
        super().__init__('GREATEST', *args, alias=alias)

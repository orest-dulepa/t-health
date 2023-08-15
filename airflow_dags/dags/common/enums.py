from enum import Flag, unique, auto


@unique
class StateEnum(Flag):
    SUCCESS = 0
    FAILURE = auto()

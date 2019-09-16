

import enum


class MType(enum.Enum):
    RESERVED = 0
    ANNOUNCE_CREATE = 1
    ANNOUNCE_DIE = 2
    LOG = 3
    TENSORBOARD = 4

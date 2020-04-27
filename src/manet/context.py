#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random as rand

from hmap.interface.context.traits.orientation import X, Y, XVel, YVel
from hmap.interface.context.traits.temporal import Time, Sleep
from hmap.interface.context.traits import random, temporal, uid
from hmap.sim.context.temporal import Clock
from hmap.sim.context.vehicles import OmniDirectionalVehicle as Vehicle


class RouterContext(uid.Bytes, random.Uniform, XVel, YVel, Time, Sleep):
    def __init__(self, *, clock, vehicle, uid):
        self.__clock = clock
        self.__v = vehicle
        self.__uid = uid
    @property
    def uid(self):
        return self.__uid
    def random_uniform(self, a, b):
        return rand.uniform(a, b)
    @property
    def x_vel(self):
        return self.__v.x_vel
    @property
    def y_vel(self):
        return self.__v.y_vel
    @property
    def time(self):
        return self.__clock.time
    def sleep(self, duration):
        self.__clock.sleep(duration)

class SimulationContext(X, Y, Time, Sleep):
    def __init__(self, *, clock, vehicle):
        self.__clock = clock
        self.__v = vehicle
    @property
    def x(self):
        return self.__v.x
    @property
    def y(self):
        return self.__v.y
    @property
    def time(self):
        return self.__clock.time
    def sleep(self, duration):
        return self.__clock.sleep(duration)

__all__  = ["RouterContext", "SimulationContext", "Clock", "Vehicle"]

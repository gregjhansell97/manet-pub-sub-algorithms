#!/usr/bin/env python
# -*- coding: utf-8 -*-

#hive-map imports
from manet.context import RouterContext, SimulationContext, Clock, Vehicle


from hmap.sim.communication import RadioTransceiver


class Transceiver(RadioTransceiver):
    def __init__(self, nid, trx_q, *args, **kwargs):
        super().__init__(**kwargs)
        self.trx_q = trx_q
        self.nid = nid
    def send(self, data, timeout=None):
        retval = super().send(data, timeout=timeout)
        self.trx_q.put(("send", self.nid, self.time, data))
        return retval
    def recv(self, timeout=None):
        data = super().recv(timeout=timeout)
        if data != b"":
            self.trx_q.put(("recv", self.nid, self.time, data))
        return data


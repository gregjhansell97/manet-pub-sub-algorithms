#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import pickle
from threading import Lock, Thread


from hmap.interface.routing import Router

class HintRouter(Router):
    def __init__(self, *, 
            matcher, context, transceiver, beacon_interval=2, credit=1
            ): 
        super().__init__(matcher=matcher)
        # communication and context
        self.__max_hint = 10
        self.__ctx = context
        self.__trx_lock = Lock()
        self.__trx = transceiver

        self.__stale = set()

        self.__credit = credit
        self.__dt = beacon_interval
        self.__nid = context.uid # node id
        # interest: uid map
        self.__nbrs_lock = Lock()
        self.__nbrs = self.Interest.Map()
        # raw_subscriptions and last beacon of neighbors
        self.__hint_table = {}

        # unique-token: msg (should not by bytes)
        self.__scheduled = {}
        # uid, timestamp
        self.__msg_timestamp = 0

        self.__recv_loop_thread = Thread(target=self.recv_loop)
        self.__recv_loop_thread.start()

    def on_beacon(self, contents):
        # extract beacon information
        nid, raw_interests = contents
        try:
            old_raw_interests, hint = self.__hint_table[nid]
        except KeyError:
            # not found need to add
            for ri in raw_interests:
                i = self.Interest.deserialize(ri)
                self.__nbrs.add(i, nid)
        else:
            # see if need to update interests
            if old_raw_interests != raw_interests: # need to update
                # remove old interests from __nbrs
                for ri in old_raw_interests:
                    i = self.Interest.deserialize(ri)
                    self.__nbrs.remove(i, nid)
                # add new interests to __nbrs
                for ri in raw_interests:
                    i = self.Interest.deserialize(ri)
                    self.__nbrs.add(i, nid)
        # reset hint table
        self.__hint_table[nid] = (raw_interests, 0) 

    def on_message(self, contents):
        current_time = self.__ctx.time
        # "each message carries a destination list composed of (id, hint)"
        # mid: message id
        # dstinations, dictionary of id: hint
        # raw_event: serialized event
        mid, destinations, credit, raw_event = contents
        if mid in self.__stale: # message has been received before
            # "removed from list of messages scheduled for transmission
            # and dropped"
            # - see if message is schedule, if so drop it 
            if mid in self.__scheduled:
                del self.__scheduled[mid]
            return # "message received, dropped without further processing"
        else: # message was never received before
            forward_delay = math.inf # whether or not to forward message
            event = self.Event.deserialize(raw_event)
            # if was never received before, then broker checks if it
            # matches some predicate into its subscription table
            num_matches = self.notify_subscriptions(event)
            assert type(num_matches) is int #TODO return num matches
            # checks if event matches some subscription, if so change
            # hint of self in destination to 0
            if num_matches > 0:
                destinations[self.__nid] = 0
                forward_delay = 0
            # furtherly broker determines if it has to reforward message
            # broker neighbors are new or not in destination
            nbrs = self.__nbrs.match(event) # find all matching neighbors
            for nid in nbrs:
                interests, last = self.__hint_table[nid]
                if last > self.__max_hint: # ignore nbrs that you haven't hear from in a while
                    continue
                try:
                    # (2) hint for nid is less than one in message
                    if destinations[nid] > last:
                        destinations[nid] = last
                        forward_delay = min(forward_delay, 0.1*last)
                except KeyError: # (1) nid doesn't belong to destinations
                    destinations[nid] = last
                    forward_delay = min(forward_delay, 0.1*last)
        if forward_delay == math.inf: # use credits instead
            # NO NEW CHANGES WERE APPLIED
            if credit > 0: # credit to send message off anyways
                credit -= 1 # decrement 
                content = (mid, destinations, credit, raw_event)
                msg = ("message", content)
                self.__scheduled[mid] = (current_time + 0.1*self.__max_hint, msg)
        else: # forward delay changed, good to send
            content = (mid, destinations, credit, raw_event)
            msg = ("message", content)
            self.__scheduled[mid] = (
                    current_time + forward_delay, msg)
        self.__stale.add(mid)

    def notify_router(self, event):
        with self.__nbrs_lock:
            destinations = {}
            nbrs = self.__nbrs.match(event)
            for nid in nbrs:
                interests, last = self.__hint_table[nid]
                if last > self.__max_hint: # ignore
                    continue
                destinations[nid] = last
            mid = (self.__nid, self.__msg_timestamp)
            raw_event = self.Event.serialize(event)
            content = (mid, destinations, self.__credit, raw_event)
            msg = ("message", content)
            messages = [("message", content)] # messages of size 1
            with self.__trx_lock:
                self.__trx.send(pickle.dumps(messages))
            self.__msg_timestamp += 1

    def close(self):
        self.__trx.close()
        self.__recv_loop_thread.join()

    def recv_loop(self):
        next_beacon_time = self.__ctx.time
        timeout = 0
        while True:
            try:
                # wait as long as beacon interval
                if timeout < 0:
                    timeout = 0
                raw_data = self.__trx.recv(timeout=timeout)
            except EOFError:
                return
            # check if next beacon time
            with self.__nbrs_lock:
                if next_beacon_time < self.__ctx.time: # increment hint table
                    next_hint_table = {}
                    for nid, v in self.__hint_table.items():
                        raw_interest, hint = v
                       # if hint < 10:
                        next_hint_table[nid] = (raw_interest, hint + 1)
                       # else: # over 10 remove from nbrs too
                       #     for ri in raw_interests:
                       #         i = self.Interest.deserialize(ri)
                       #         self.__nbrs.remove(i, nid)
                    self.__hint_table = next_hint_table
                    # reset beacon time
                    next_beacon_time = self.__ctx.time + self.__dt
                    # schedule beacon message
                    raw_interests = tuple(
                            self.Interest.serialize(i) 
                            for i in self.local_interests)
                    if len(raw_interests) > 0: # has something worth beaconing
                        content = (self.__nid, raw_interests)
                        msg = ("beacon", content)
                        # schedule beacon
                        self.__scheduled[str(self.__nid)] = (
                                self.__ctx.time, msg)
                if len(raw_data) > 0: # received data
                    # extract data
                    messages = pickle.loads(raw_data) # list of messages
                    for raw_data in messages:
                        channel, content = raw_data
                        if channel == "beacon":
                            self.on_beacon(content) # update tables
                        elif channel == "message":
                            self.on_message(content)
                # send off any scheduled messages
                current_time = self.__ctx.time
                messages = []
                next_schedule = {}
                timeout = next_beacon_time - current_time
                for k, v in self.__scheduled.items():
                    t, m = v
                    if t <= current_time:
                        messages.append(m)
                    else:
                        timeout = min(timeout, t - current_time)
                        next_schedule[k] = (t, m)
                self.__scheduled = next_schedule
                messages = tuple(messages)
                #messages = tuple(
                #        msg 
                #        for t, msg in self.__scheduled.values() 
                #        if t <= current_time)
                # send messages globbed together
                if len(messages) > 0:
                    with self.__trx_lock:
                        self.__trx.send(pickle.dumps(messages))
                # filter out old scheduled items
                #self.__scheduled = {
                #        k: v
                #        for k, v in self.__scheduled.items()
                #        if v[0] > current_time}
        



"""
        uid, mid, targets, raw_event = pickle.loads(contents)
        event = self.Event.deserialize(raw_event)
        matches = set(self.__hint_table.match(event))
        current_time = self.__ctx.time

        # target id and last heard
        # iterage through targets
        # goal populate next_targets (to send out)
        next_targets = []
        delay = 0
        for target, t_beacon in targets: 
            # t_beacon is a number between 0 and max_beacon
            credit = 0
            tid = (uid, mid, target)
            target = tid[2]
            if tid in self.__stale: # already seen message
                if tid in self.__pending: # wating to send out message
                    self.__pending[tid][
                    if self.__pending[tid] >
                    del self.__pending[tid] # got beaten to punchline
            else: # have not seen message
                if target == self.uid: # you are a target
                    self.next_targets.append(
                            (current_time, self.uid, self.__beacon))
                    self.notify_subscriptions(event)
                elif target in matches: # target matches locals
                    _, last_beacon, last_heard, _ = self.__last_heard[target]
                    # beacon received is further away
                    if t_beacon < last_beacon:
                        # TODO ADJUST RATIO
                        pause = 0.1*(current_time - last_heard)
                        send_time = pause + current_time
                        next_targets.append(
                                (send_time, target, last_beacon))
                    matches.remove(target)
                else: # target not in locals or self
                    if credit > 0: # has credit to be sent anyways 
                        # TODO send anyways
                        pass
                # have seen message now!
                self.__stale.add(tid)
        # go through matches that have not been removed
        for target in matches:
            _, last_beacon, last_heard, _ = self.__last_heard[target]
            pause = 0.1*(current_time - last_heard)
            send_time = pause + current_time
            next_targets.append(send_time, target, last_beacon)



        # make targets and other targets exlusive
        # unless target, don't send *right* away
        # tid = (node id, message id, target)
        for tid, b_clock in targets:
            if (nid, mid, tid) in pending:
                e, t, bc = pending[(nid, mid, tid)]
                if bc >= b_clock: # beaten to it, neutralize pending
                    del pending[(nid, mid, tid)]
                # TODO received message waiting to be sent out
                # verify higher b_clock
            if (nid, mid, target_id) in self.__stale:
                # already received target
                # TODO check pending
                
                return
            elif target_id == self.uid: # target reached
                #TODO lock
                # retransmit, telling all others to hush (largets possible bc)
                # get them to add message to nid, mid, target_id
                #self.__trx.send((
            elif tid in other_targets: # knows a target...
                # puts tid on pending
                # removes from other targets
                pass
        # send targets that did not get filtered out on pending

        # UPDATES SUBSCRIPTION TABLE
        # clear out old interests
        try:
            old_interests, t, exp_time = self.__last_heard[uid]
        except KeyError:
            pass
        else:
            # TODO compare old to new, may be able to make into set/compare
            for i in old_interests:
                self.__hint_table.remove(i, uid)

        # create new list of interests
        interests = [] # interests
        for ri in raw_interests:
            i = self.Interest.deserialize(ri)
            interests.append(i)
            self.__hint_table.add(i, uid)
        # stamp with newly updated time
        t = self.__ctx.time
        self.__last_heard[uid] = (
                interests, beacon, t, t + duration)
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
from multiprocessing import Barrier, Process, Queue
import pickle
import queue
import random as rand
from threading import Thread
import time
import uuid

#hive-map imports
from hmap.std import Node
from hmap.std.matching import TopicBasedMatcher
from hmap.std.routing import GossipRouter

from manet.context import RouterContext, SimulationContext, Clock, Vehicle
from manet import radio
from manet.hint_router import HintRouter




def random_waveform_worker(*, 
        start_barrier, 
        node_ids,
        subs, # dictionary of nids and [topics] they are subscribed to
        pubs, # list of node-id, topic and time of publish
        paths, # path of each node len(path) == num-nodes
        clock_speed,
        simulation_duration,
        trx_kwargs, 
        create_router,
        sub_logs_q,
        pub_logs_q,
        trx_logs_q):
    clock = Clock(speed=clock_speed)
    trxs = {}
    vehicles = {}
    routers = {}
    for nid in node_ids:
        x, y = (paths[nid][0][0], paths[nid][0][1])
        v = Vehicle(x, y)
        # context
        sim_context = SimulationContext(clock=clock, vehicle=v)
        router_context = RouterContext(clock=clock, vehicle=v, uid=nid)
        # hive-map components
        matcher = TopicBasedMatcher("FlatInt", "PyObj")
        trx = radio.Transceiver(
                nid, trx_logs_q, context=sim_context, **trx_kwargs)
        router = create_router(
                context=router_context,
                matcher=matcher,
                transceiver=trx)
        # component stores
        trxs[nid] = trx
        vehicles[nid] = v
        routers[nid] = router
    nodes = {nid: Node(r) for nid, r in routers.items()}    

    # SUBSCRIPTIONS
    def get_callback():
        def cb(t, msg):
            cb.log.append((clock.time, msg))
        cb.log = []
        return cb
    subscriptions = {nid: [] for nid in subs.keys()}
    for nid, topics in subs.items():
        subscriptions[nid] = [
                nodes[nid].subscribe(t, get_callback()) 
                for t in topics]
    #BARRIER
    start_barrier.wait()
    clock.start()
    pub_logs = {}
    while clock.time < simulation_duration: # run for duration of simulation
        clock.sleep(0.001)
        if len(pubs) > 0:
            nid, topic, t = pubs[0]
            tc = clock.time
            # new item to publish
            if t < tc:
                nodes[nid].publish(topic, (nid, tc)) #TODO make publish non-sleeping
                try:
                    pub_logs[nid].append((topic, tc))
                except KeyError:
                    pub_logs[nid] = [(topic, tc)]
                pubs.pop(0) # success, remove item!
        for nid in node_ids:
            path = paths[nid]
            tc = clock.time
            x1, y1, t1 = path[1]
            while tc > t1:
                path.pop(0)
                x1, y1, t1 = path[1]
            x0, y0, t0 = path[0]
            r = (tc - t0)/(t1 - t0) 
            x = x0 + r*(x1 - x0)
            y = y0 + r*(y1 - y0)
            v = vehicles[nid]
            v.set_x(x)
            v.set_x_vel((x1 - x0)/(t1 - t0))
            v.set_y(y)
            v.set_y_vel((y1 - y0)/(t1 - t0))
    # clean-up
    for n in nodes.values():
        n.close()

    # collect log data
    sub_logs = {}
    for nid, s_list in subscriptions.items():
        sub_logs[nid] = {s.topic: s.callback.log for s in s_list}

    send_logs = {}
    for nid, t in trxs.items():
        send_logs[nid] = t.send_log

    recv_logs = {}
    for nid, t in trxs.items():
        recv_logs[nid] = t.recv_log

    pub_logs_q.put(pub_logs)
    sub_logs_q.put(sub_logs)

def create_gossip_router(**kwargs):
    return HintRouter(**kwargs, beacon_interval=5, credit=2)
    #return GossipRouter(**kwargs, gossip_level=1)

def random_waveform_benchmark():
    title = "hint_2_seed_47"
    rand.seed(47)
    # PARAMETERS
    # VEHICLE
    vehicle_speed = (10, 20) # meters per second
    # BOUNDARY
    x_bound = (0, 1000) # meters
    y_bound = (0, 1000) #meters
    # TIME
    clock_speed = 0.5# second per second
    simulation_duration = 125 #60 # seconds
    # PARTICIPANTS
    # two publishers
    # first 20 publish
    #pubs = [(i%100, 1, i//2 + 0.2) for i in range(120)]
    pubs = [(i, 1, i*2 + 0.2) for i in range(60)]
    # 10 subscribers
    #subs = {i: [1] for i in range(20, 30)} # node i is interested in topic 1
    subs = {i: [1] for i in range(60, 70)} # node i is interested in topic 1
    node_ids = list(range(100))
    #node_ids = list(range(10))
    # MULTIPROCESSING
    num_processes = 4
    start_barrier = Barrier(num_processes)
    # TRANSCEIVER
    trx_kwargs = {
            "world": str(uuid.uuid4()),
            "data_rate": 2000, #kbps
            "send_range": 75,
            "recv_range": 75,
            "max_buffer_size": 1024
    }
    paths = {}
    for nid in node_ids:
        # sets path at a random point in time
        path = [(rand.uniform(*x_bound), rand.uniform(*y_bound), 0)]
        while path[-1][2] <= simulation_duration + 5:
            speed = rand.uniform(*vehicle_speed)
            x, y, t = path[-1]
            next_x = rand.uniform(*x_bound)
            next_y = rand.uniform(*y_bound)
            distance = math.sqrt((x - next_x)**2 + (y - next_y)**2)
            next_t = distance/speed + t
            path.append((next_x, next_y, next_t))
        paths[nid] = path

    pub_logs_q = Queue()
    sub_logs_q = Queue()
    trx_logs_q = Queue()
    workers = []
    ###########################################################################
    '''
    nids = node_ids[0::num_processes] # divy up nodes
    p_kwargs = {
            "start_barrier": start_barrier,
            "node_ids": nids,
            "subs": {nid: v for nid, v in subs.items() if nid in nids},
            "pubs": [(nid, t, pt) for nid, t, pt in pubs if nid in nids],
            "paths": {nid: v for nid, v in paths.items() if nid in nids},
            "clock_speed": clock_speed,
            "simulation_duration": simulation_duration,
            "trx_kwargs": trx_kwargs,
            "create_router": create_gossip_router,
            "sub_logs_q": sub_logs_q,
            "pub_logs_q": pub_logs_q,
            "trx_logs_q": trx_logs_q
    }
    random_waveform_worker(**p_kwargs)
    send_logs = {}
    recv_logs = {}
    '''
    ############################################################################
    for i in range(num_processes):
        nids = node_ids[i::num_processes] # divy up nodes
        p_kwargs = {
                "start_barrier": start_barrier,
                "node_ids": nids,
                "subs": {nid: v for nid, v in subs.items() if nid in nids},
                "pubs": [(nid, t, pt) for nid, t, pt in pubs if nid in nids],
                "paths": {nid: v for nid, v in paths.items() if nid in nids},
                "clock_speed": clock_speed,
                "simulation_duration": simulation_duration,
                "trx_kwargs": trx_kwargs,
                "create_router": create_gossip_router,
                "sub_logs_q": sub_logs_q,
                "pub_logs_q": pub_logs_q,
                "trx_logs_q": trx_logs_q
        }
        p = Process(target=random_waveform_worker, kwargs=p_kwargs)
        workers.append(p)
    for w in workers:
        w.start()
    send_logs = {nid: [] for nid in node_ids}
    recv_logs = {nid: [] for nid in node_ids}
    while any([w.is_alive() for w in workers]):
        try:
            channel, nid, time, data = trx_logs_q.get(timeout=0.25)
            if channel == "send": 
                send_logs[nid].append((time, data))
            elif channel == "recv":
                recv_logs[nid].append((time, data))
            else:
                assert False
        except queue.Empty:
            pass

    # wait to join processes
    for w in workers:
        w.join()


    while not trx_logs_q.empty:
        channel, nid, time, data = trx_logs_q.get(timeout=0.25)
        if channel == "send": 
            send_logs[nid].append((time, data))
        elif channel == "recv":
            recv_logs[nid].append((time, data))
        else:
            assert False
    print("JOINED WORKERS")

    pub_logs = {}
    while not pub_logs_q.empty():
        pub_logs = {**pub_logs, **pub_logs_q.get()}

    sub_logs = {}
    while not sub_logs_q.empty():
        sub_logs = {**sub_logs, **sub_logs_q.get()}

    print("PUB LOGS")
    for nid, data in pub_logs.items():
        print(f"{nid}: {data}")
    print("\n")
    print("SUB LOGS")
    for nid, topics in sub_logs.items():
        print(f"{nid}")
        for t, log in topics.items():
            print(f"\t{t}: {len(log)}")
    print("\n")
    total_bytes = 0
    num_sends = 0
    for nid, log in send_logs.items():
        total_bytes += sum([len(data) for t, data in log])
        num_sends += len(log)
        #print(f"{nid}: {total_bytes}")
    print(f"Bytes Transmitted: {total_bytes}")
    print(f"Num Sends: {num_sends}")

    logs = {
            "pub": pub_logs,
            "sub": sub_logs,
            "send": send_logs,
            "recv": recv_logs,
            "paths": paths
        }
    with open(f"{title}.pickle", "xb") as f:
        pickle.dump(logs, f)
    print("ALL DONE :)")

    return






















"""
        # vehicle initially has same position as goal (not moving)
        vehicle = Vehicle(path[0][0], path[0][1])

        # CONTEXT 
        # context available to simulated components
        sim_context = SimulationContext(
                clock=clock, 
                vehicle=vehicle)
        # context available to router
        router_context = RouterContext(
                clock=clock, 
                vehicle=vehicle, 
                uid=uid)
        # MATCHER
        matcher = TopicBasedMatcher("FlatInt", "PyObj")
        # TRANSCEIVER
        trx = RadioTransceiver(context=sim_context, **trx_kwargs)
        # ROUTER
        router = GossipRouter(
                context=router_context,
                matcher=matcher,
                transceiver=trx,
                gossip_level=1)
        # add to lists
        trxs.append(trx)
        routers.append(router)
        vehicles.append(vehicle)
        paths.append(path)

    nodes = [Node(r) for r in routers]


    print("SETTING UP PUBLISHERS")
    # EXTRACT PUBLISHERS AND SUBSCRIBERS
    # PUBLISHERS
    def publish(node, *, id_, delay=0, period=1, duration=10):
        clock.sleep(delay)
        msg_number = 0
        while clock.time <= duration:
            t = period + clock.time
            node.publish(1, (id_, msg_number, clock.time))
            pub_logs[id_].append((id_, msg_number, clock.time)) 
            t -= clock.time 
            if t > 0:
                clock.sleep(t) #waits
            msg_number += 1
    publishers = nodes[:num_pubs] 
    pub_threads = [] # list of publisher threads
    for p, i in zip(publishers, range(len(publishers))):
        kwargs = {
                "id_": i,
                "delay": publish_delay*i,
                "duration": publish_duration
        }
        pub_threads.append(Thread(target=publish, args=[p], kwargs=kwargs))
        pub_threads[-1].start()

    print("SETTING UP SUBSCRIBERS")
    # SUBSCRIBERS
    def get_callback(id_):
        def cb(t, msg):
            sub_logs[id_].append((clock.time, msg))
        return cb
    subscribers = [
            (s, get_callback(i)) 
            for s, i in zip(nodes[-num_subs:], range(num_subs))]
    # assign callbacks to subscribers
    for s, cb in subscribers:
        s.subscribe(1, cb)

    time.sleep(1)

    clock.start()


    print("STARTING SIMULATION")
    while clock.time < simulation_duration: # run for duration of simulation
        clock.sleep(0.01)
        for v, path in zip(vehicles, paths):
            tc = clock.time
            x1, y1, t1 = path[1]
            while tc > t1:
                path.pop(0)
                x1, y1, t1 = path[1]
            x0, y0, t0 = path[0]
            r = (tc - t0)/(t1 - t0) 
            x = x0 + r*(x1 - x0)
            y = y0 + r*(y1 - y0)
            v.set_x(x)
            v.set_x_vel((x1 - x0)/(t1 - t0))
            v.set_y(y)
            v.set_y_vel((y1 - y0)/(t1 - t0))
    # CLEAN-UP TEST IS DONE
    # close nodes
    for n in nodes:
        n.close()
    # join threads
    for t in pub_threads:
        t.join()

    #print(pub_logs)
    for k, v in pub_logs.items():
        print(f"{k}: {v}")
    print("")
    #print(sub_logs)
    for k, v in sub_logs.items():
        v = [m[1] for t, m in v]
        print(f"{k}: {v}")
"""

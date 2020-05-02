#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np
import pickle



def get_pickles(): 
    files = {
            "gossip 0.1": "./pickles/gossip_0_1_seed_47.pickle",
            "gossip 0.3": "./pickles/gossip_0_3_seed_47.pickle",
            "gossip 0.5": "./pickles/gossip_0_5_seed_47.pickle",
            "gossip 0.7": "./pickles/gossip_0_7_seed_47.pickle",
            "gossip 0.9": "./pickles/gossip_0_9_seed_47.pickle",
            "hint 0": "./pickles/hint_0_seed_47.pickle",
            "hint 1": "./pickles/hint_1_seed_47.pickle",
            "hint 2": "./pickles/hint_2_seed_47.pickle"
        }
    results = {}
    for title, fname in files.items():
        with open(fname, "rb") as f:
            results[title] = pickle.load(f)
    return results

"""
WHATS INSIDE EACH PICKLE FILE?
trial-name:
    pub:
        node_id: [(topic, time), ...]
    sub:
        node_id [20 - 29]:
            topic: [time-rcvd, (sender, time-send)]
    send:
        node_id: [(time, bytes-sent)]
    recv:
        node_id: [(time, bytes_rcvd)]
    paths
"""

def plot_recv_max():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        msgs_rcvd = []
        for topic_rcvd in data["sub"].values():
            # go through each topic get latency of each message
            node_msgs_rcvd = 0
            for rlist in topic_rcvd.values():
                node_msgs_rcvd += len(rlist)
            msgs_rcvd.append(node_msgs_rcvd)
        cost[title] = max(msgs_rcvd)
    # remove participants
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Max Subscription Receives", 
            xlabel="Protocols", 
            ylabel="Number of Events Received")

def plot_recv_min():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        msgs_rcvd = []
        for topic_rcvd in data["sub"].values():
            # go through each topic get latency of each message
            node_msgs_rcvd = 0
            for rlist in topic_rcvd.values():
                node_msgs_rcvd += len(rlist)
            msgs_rcvd.append(node_msgs_rcvd)
        cost[title] = min(msgs_rcvd)
    # remove participants
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Max Subscription Receives", 
            xlabel="Protocols", 
            ylabel="Number of Events Received")

def plot_recv_fairness():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        msgs_rcvd = []
        for topic_rcvd in data["sub"].values():
            # go through each topic get latency of each message
            node_msgs_rcvd = 0
            for rlist in topic_rcvd.values():
                node_msgs_rcvd += len(rlist)
            msgs_rcvd.append(node_msgs_rcvd)
        cost[title] = max(msgs_rcvd) - min(msgs_rcvd)
    # remove participants
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Max-Min Receives Difference", 
            xlabel="Protocols", 
            ylabel="Number of Events Received")


def plot_cost_min():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        num_sends = []
        for nid, send_log in data["send"].items():
            num_sends.append(len(send_log))
        cost[title] = min(num_sends)
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Min Cost", 
            xlabel="Protocols", 
            ylabel="'Send' Operations")
def plot_cost_max():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        num_sends = []
        for nid, send_log in data["send"].items():
            num_sends.append(len(send_log))
        cost[title] = max(num_sends)
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Max Cost", 
            xlabel="Protocols", 
            ylabel="'Send' Operations")
def plot_cost_fairness():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        num_sends = []
        for nid, send_log in data["send"].items():
            num_sends.append(len(send_log))
        cost[title] = max(num_sends) - min(num_sends)
    del cost["gossip 0.1"]
    del cost["gossip 0.3"]
    plot_results(
            cost, 
            title="Routing Protocol Max-Min Cost Difference", 
            xlabel="Protocols", 
            ylabel="'Send' Operations")


def plot_cost():
    trials = get_pickles()
    cost = {}
    for title, data in trials.items():
        num_sends = 0
        for nid, send_log in data["send"].items():
            num_sends += len(send_log)
        assert len(data["send"]) == 100
        cost[title] = num_sends/len(data["send"])
    plot_results(
            cost, 
            title="Routing Protocol Average Cost per Node", 
            xlabel="Protocols", 
            ylabel="'Send' Operations")





def plot_reliability():
    trials = get_pickles()
    reliability = {}
    for title, data in trials.items():
        num_pubs = 0
        for plist in data["pub"].values():
            num_pubs += len(plist)
        #assert num_pubs == 120
        num_successes = 0
        for topic_rcvd in data["sub"].values():
            num_successes += sum([len(r) for r in topic_rcvd.values()])
        #assert len(data["sub"]) == 10
        average_num_successes = num_successes/len(data["sub"])
        reliability[title] = average_num_successes/num_pubs

    plot_results(
            reliability, 
            title="Routing Protocol Average Reliability per Subscriber", 
            xlabel="Protocols", 
            ylabel="Reliability")


def plot_latency():
    trials = get_pickles()
    latency = {}
    for title, data in trials.items():
        total_latency = 0
        num_msgs = 0
        for topic_rcvd in data["sub"].values():
            # go through each topic get latency of each message
            for rlist in topic_rcvd.values():
                total_latency += sum([r[0] - r[1][1] for r in rlist])
                num_msgs += len(rlist)
        latency[title] = total_latency/num_msgs
    plot_results(
            latency,
            title="Routing Protocol Average Latency per Subscriber",
            xlabel="Protocols",
            ylabel="Latency (seconds)")




def plot_results(results, xlabel=None, ylabel=None, title=None):
    results = [(v, k) for k, v in results.items()]
    results.sort()
    styles = {
            "gossip 0.1": ("b", 0.1),
            "gossip 0.3": ("b", 0.25),
            "gossip 0.5": ("b", 0.5),
            "gossip 0.7": ("b", 0.75),
            "gossip 0.9": ("b", 1),
            "hint 0": ("g", 0.2),
            "hint 1": ("g", 0.6),
            "hint 2": ("g", 1)
        }

    fig, ax = plt.subplots()
    bar_width = 0.75

    handles = []
    for i, r in zip(range(len(results)), results):
        v, t = r
        color, alpha = styles[t]
        if "gossip" in t:
            catagory = "gossip"
        else:
            catagory = "hint"
        handles.append(
                (t, 
                plt.bar(i, v, bar_width, 
                    alpha=alpha, color=color, label=t)))
    handles.sort()
    labels = [h[0] for h in handles]
    rects = [h[1] for h in handles]

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks([])
    plt.legend(rects, labels)

    plt.tight_layout()
    plt.show()

#!/usr/bin/env python3

from collections import defaultdict
import os
import time
import sys

import boto3

import minions
from pool import Pool

ec2 = boto3.resource('ec2')

# left here by Ansible who got it from Terraform
cluster_name = open(os.path.expanduser('~/.cluster_name')).read().strip()

large_minions = minions.track_down_minions(
    ec2,
    dict(cluster=cluster_name,
         cluster_groups='minions',
         size='large'))
small_minions = minions.track_down_minions(
    ec2,
    dict(cluster=cluster_name,
         cluster_groups='minions',
         size='small'))

large_pool = Pool("LARGE", large_minions)
small_pool = Pool("SMALL", small_minions)

# maps host names to list of open claims
CLAIMS = defaultdict(list)


def print_pool(pool):
    may_shrink = ", postponing shrinks" if pool.postpone_shrink else ""
    print(
        f"Pool {pool.name}, actual={pool.actual}, desired={pool.desired}{may_shrink}:", )
    for i, (name, state, claims, minion) in enumerate(pool.members()):
        state_suffix = f"/{claims}" if claims else ""
        print(f"{i+1:2d} {name} state={state}{state_suffix}", end="")
        observed = minion.observed_state
        desired = minion.desired_state
        if observed == desired:
            print(f" minion={minion.observed_state}", end="")
        else:
            print(
                f" minion={minion.observed_state}->{minion.desired_state}", end="")
        print()


class Delayer:
    def __init__(self, delay):
        self.next_time = 0
        self.delay = delay

    def await(self):
        now = time.time()
        sleep = self.next_time - now
        if sleep > 0:
            time.sleep(sleep)
        self.next_time = now + self.delay


delayer = Delayer(1)
quick = False
while 1:
    if quick:
        print('---')
        quick = False
    else:
        delayer.await()
        print("===")
        small_pool.allow_shrink = large_pool.actual >= large_pool.desired
        large_pool.allow_shrink = small_pool.actual >= small_pool.desired
        large_pool.poll()
        small_pool.poll()

    print_pool(large_pool)
    print()
    print_pool(small_pool)
    print()
    minions_with_claims = []
    if any(CLAIMS.values()):
        print("Claims:", end="")
        for i, (name, claims) in enumerate(sorted(CLAIMS.items())):
            if not claims:
                continue
            minions_with_claims.append(name)
            n = len(claims)
            print(f"  {i+1}=", end="")
            if n == 1:
                print(name, end="")
            elif n > 1:
                print(f"{n}*{name}", end="")
        print()

    cmd = input("> ").strip()
    if cmd.startswith("c"):
        if cmd.startswith("cl"):
            pool = large_pool
        elif cmd.startswith("cs"):
            pool = small_pool
        else:
            print("Either cl or cs")
            continue
        c = pool.claim()
        CLAIMS[c.name].append(c)
        print(f"Claimed {c.name}")
        quick = True
    elif cmd.startswith("q"):
        sys.exit(0)
    elif cmd.startswith("r"):
        try:
            idx = int(cmd[1:]) - 1
            if idx >= len(minions_with_claims):
                raise ValueError("")
        except ValueError:
            print("Invalid index")
            continue
        name = sorted(minions_with_claims)[idx]
        claim = CLAIMS[name].pop()
        claim.release()
        print(f"Released one claim on {name}")
    else:
        # grow/shrink the pools
        for s in cmd:
            if s.startswith("s"):
                print("Shrink SMALL")
                small_pool.desired -= 1
            elif s.startswith("S"):
                print("Grow SMALL")
                small_pool.desired += 1
            elif s.startswith("l"):
                print("Shrink LARGE")
                large_pool.desired -= 1
            elif s.startswith("L"):
                print("Grow LARGE")
                large_pool.desired += 1
            elif s == "":
                pass
            else:
                print("Unknown command", s)

    # Do not allow one pool to shrink while the other is growing.
    # This way, a minion is killed only when its replacement has started

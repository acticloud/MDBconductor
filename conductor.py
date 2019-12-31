#!/usr/bin/env python3

import time

import boto3
from minions import READY, RUNNING, STOPPED, track_down_minions

ec2 = boto3.resource('ec2')

# Just take any existing minions.
# We recognize our minions by the tag MinionOf=joeri.
# This should be generalized of course.
MY_MINIONS = sorted(
    track_down_minions(ec2, dict(MinionOf='joeri')),
    key=lambda m: m.name)


def show_status():
    now = time.time()
    for i, m in enumerate(MY_MINIONS):
        print(f"{i+1:2} {m.name:>10}/{m.id:20}", end="")
        if not m.desired_state or m.desired_state == m.observed_state:
            print(m.observed_state, end="")
        else:
            how_long = now - m.last_action_time
            print(f"{m.observed_state}->{m.desired_state} since {how_long:.1f}s")
        print()


def get_command():
    # hacky hacky
    commandline = input("> ").strip()
    if not commandline:
        return '', []

    words = commandline.split()
    command = words[0].lower()

    targets = set()
    for arg in words[1:]:
        if arg == 'all':
            targets = targets.union(set(MY_MINIONS))
            continue
        try:
            n = int(arg)
        except ValueError:
            # will be rejected below
            n = len(MY_MINIONS) + 1000
            continue
        if n > len(MY_MINIONS):
            print(f"Please give numbers 1..{len(MY_MINIONS)}")
            return '', []

        targets.add(MY_MINIONS[n - 1])

    return command, targets


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
while 1:
    delayer.await()
    for minion in MY_MINIONS:
        minion.poll()

    print()
    show_status()

    command, targets = get_command()
    desired = None
    if command in ['', 'status']:
        continue
    elif command == 'up':
        desired = READY
    elif command == 'down':
        desired = STOPPED
    else:
        print(f"Unknown command: {command}")
        continue
    
    assert desired != None
    for minion in targets:
        if not minion.make(desired):
            print(f"Error: cannot make {minion.name} {desired}")

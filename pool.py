#

from collections import defaultdict
import random
import threading
import time

from minions import Minion, NONEXISTENT, PENDING, RUNNING, SHUTTING_DOWN, TERMINATED, STOPPING, STOPPED, READY


class Pool:

    def __init__(self, name, members):
        self._lock = threading.Lock()
        self.name = name
        self._by_name = {}
        self._state = {}
        self._generation = {}
        self._claims = {}
        self._desired_up = 0
        self.shrink_allowed = True
        self.loadaverage = LoadAverage()

        for m in members:
            name = m.name
            m.refresh()
            mstate = m.desired_state or m.observed_state

            if mstate == RUNNING or mstate == READY:
                state = 'UP'
            elif mstate == 'PENDING':
                state = 'STARTING'
            else:
                state = 'DOWN'

            self._by_name[name] = m
            self._state[name] = state
            self._generation[name] = 0
            self._claims[name] = 0
            # sends command to the minion
            self._set_member_state(name, state)

        # Assume this is just how we want it.
        # User will probably call set_des
        self._desired_up = sum(1 for st in self._state.values()
                               if st in ['UP', 'STARTING'])

    def members(self):
        with self._lock:
            return [
                (name, self._state[name], self._claims[name], member)
                for name, member in self._by_name.items()
            ]

    def poll(self):
        # Change our state info based on the observed minion state
        with self._lock:
            for name, member in self._by_name.items():
                member.refresh()
                state = self._state[name]
                observed = member.observed_state
                # the following is so ad-hoc that it's almost certainly
                # wrong :(
                if state == 'STARTING' and observed == READY:
                    self._generation[name] += 1
                    self._claims[name] = 0
                    self._set_member_state(name, 'UP')
                if state == 'UP' and observed != READY:
                    self._set_member_state(name, 'STARTING')

            # Try to grow and shrink
            self._up_rule()
            if self.shrink_allowed:
                self._down_rule()

            # Tell the minions what we think they ought to do
            for name, minion in self._by_name.items():
                state = self._state[name]
                if state in ['STARTING', 'UP', 'FINISHING']:
                    minion.make(READY)
                elif state in ['DOWN']:
                    minion.make(STOPPED)
                else:
                    assert('banana')
            for name, minion in self._by_name.items():
                minion.poll()

    def _actual(self):
        with self._lock:
            return sum(1 for s in self._state.values() if s in ['UP', 'FINISHING'])

    actual = property(_actual)

    def _get_desired(self):
        with self._lock:
            return self._desired_up

    def _set_desired(self, n):
        with self._lock:
            if n < 0:
                n = 0
            if n > len(self._by_name):
                n = len(self._by_name)
            self._desired_up = n

    desired = property(_get_desired, _set_desired)

    def _set_postpone_shrink(self, b):
        with self._lock:
            self.shrink_allowed = not b

    def _get_postpone_shrink(self):
        with self._lock:
            return not self.shrink_allowed

    postpone_shrink = property(
        _get_postpone_shrink,
        _set_postpone_shrink,
    )

    def _set_member_state(self, name, state):
        assert(name in self._state)
        assert(state in ['STARTING', 'UP', 'FINISHING', 'DOWN'])
        oldstate = self._state[name]
        self._state[name] = state
        if oldstate != state:
            claims = self._claims[name]
            print(f"~ ({name} now {state}/{claims})")
            result = True
        else:
            result = False
        member = self._by_name[name]
        if state in ['STARTING', 'UP']:
            member.make(READY)
        else:
            member.make(STOPPED)

    def classify(self):
        result = defaultdict(list)
        for name, state in self._state.items():
            claimed = self._claims[name] > 0
            key = (state, claimed)
            # long live dynamic typing
            result[state].append(name)
            result[key].append(name)
        return result

    def _up_rule_once(self):
        cfy = self.classify()
        starting = cfy['STARTING']
        up = cfy['UP']

        if len(starting) + len(up) >= self._desired_up:
            # no need to change anything
            return False

        finishing = cfy['FINISHING']
        if finishing:
            # take one that was on the list to go down and undo that decision
            return self._set_member_state(finishing[0], 'UP')

        down = cfy['DOWN']
        assert(down)
        return self._set_member_state(down[0], 'STARTING')

    def _up_rule(self):
        changed = self._up_rule_once()
        if not changed:
            return False
        while self._up_rule_once():
            pass
        return True

    def _down_rule_once(self):
        cfy = self.classify()
        starting = cfy['STARTING']
        up = cfy['UP']

        if len(starting) + len(up) <= self._desired_up:
            # no need to change anything
            return False

        up0 = cfy[('UP', False)]
        if up0:
            # this idle minion can be stopped instantly
            return self._set_member_state(up0[0], 'DOWN')

        starting = cfy['STARTING']
        if starting:
            # better to kill a starting node because we don't have to
            # wait for the queries do drain
            return self._set_member_state(starting[0], 'DOWN')

        upn = cfy[('UP', True)]
        # there were too many nodes either starting or up.
        # they weren't any STARTING nodes and there weren't
        # any UP(n==0) nodes so there must be at least one UP(n>0).
        assert(upn)
        return self._set_member_state(upn[0], 'FINISHING')

    def _down_rule(self):
        changed = self._down_rule_once()
        if not changed:
            return False
        while self._down_rule_once():
            pass
        return True

    def claim(self):
        with self._lock:
            cfy = self.classify()
            ups = cfy['UP']
            if not ups:
                return None
            victim = random.choice(ups)
            self._claims[victim] += 1
            self.loadaverage.add_load(1)
            return Claim(self, victim, self._by_name[victim].ip, self._generation[victim])

    def _release(self, name, generation):
        with self._lock:
            if self._generation[name] != generation:
                return
            claims = self._claims[name]
            assert claims > 0
            self._claims[name] -= 1
            self.loadaverage.remove_load(1)
            if claims == 1:
                # now 0
                if self._state[name] == 'FINISHING':
                    self._set_member_state(name, 'DOWN')


class Claim:
    def __init__(self, pool, name, ip, generation):
        self.name = name
        self.ip = ip
        self._pool = pool
        self._generation = generation
        self._released = False

    def release(self):
        if not self._released:
            self._pool._release(self.name, self._generation)
            self._released = True

    def __enter__(self):
        if self._released:
            raise Exception("Claim has already been released")
        return self

    def __exit__(self, _type, _value, _traceback):
        self.release()


class LoadAverage:
    def __init__(self, half_life=60):
        self.half_life = half_life
        self._alpha = 0.5 ** (1.0 / half_life)
        self._load = 0
        self._echo = 0

        now = time.time()
        self._start_time = now
        self._last_change = now
        self._last_echo_update = now

    @property
    def load(self):
        now = time.time()
        elapsed = now - self._last_echo_update
        echo = self._echo * self._alpha ** elapsed
        return self._load + echo

    @property
    def time_since_change(self):
        now = time.time()
        return now - self._last_change

    @property
    def time_running(self):
        now = time.time()
        return now - self._start_time

    def _update_echo(self, now):
        elapsed = now - self._last_echo_update
        self._echo *= self._alpha ** elapsed
        self._last_echo_update = now

    def add_load(self, amount):
        assert amount >= 0
        now = time.time()
        self._last_change = now

        self._update_echo(now)
        self._load += amount
        self._echo -= amount
        if self._echo < 0:
            self._echo = 0

    def remove_load(self, amount):
        assert amount >= 0
        now = time.time()
        self._last_change = now
        if amount > self._load:
            amount = self._load

        self._update_echo(now)
        self._load -= amount
        self._echo += amount

    def adjust_load(self, amount):
        if amount > 0:
            self.add_load(amount)
        else:
            self.remove_load(-amount)

    def set_load(self, load):
        self.adjust_load(load - self._load)

#

import socket
import time

from rule_engine import Rule, RuleEngine


class State:
    """Wrapper around Amazons instance status codes.

    If .destination_code is set, this state is expected
    to eventually turn into that state. For example,
    STOPPING eventually becomes STOPPED.

    Use state_from_code to get the State object for a given code.
    """

    def __init__(self, name, code, destination_code=None):
        self.name = name
        self.code = code
        self.destination_code = destination_code

    def __str__(self):
        return self.name


NONEXISTENT = State('NONEXISTENT', -1)
PENDING = State('PENDING', 0, 16)
RUNNING = State('RUNNING', 16)
SHUTTING_DOWN = State('SHUTTING_DOWN', 32, 48)
TERMINATED = State('TERMINATED', 48)
STOPPING = State('STOPPING', 64, 80)
STOPPED = State('STOPPED', 80)
#
READY = State('READY', RUNNING.code + 1)

_codemap = {
    0: PENDING,
    16: RUNNING,
    32: SHUTTING_DOWN,
    48: TERMINATED,
    64: STOPPING,
    80: STOPPED,
}


def state_from_code(n):
    """Map EC2 instance state code to its corresponding State object."""
    st = _codemap.get(n)
    if not st:
        raise Exception(f"No state has code {n}")
    return st


class Minion:
    """Information about current and desired state of a minion, and what
    we're doing about it.
    """

    engine = RuleEngine([
        Rule([STOPPED], PENDING, "start"),
        Rule([PENDING], RUNNING, "wait"),
        Rule([RUNNING], READY, "wait"),
        Rule([RUNNING, READY], STOPPING, "stop"),
        Rule([STOPPING], STOPPED, "wait")
    ])

    def __init__(self, ec2, name, id=None):
        self.ec2 = ec2
        self.name = name
        self.id = id

        self.observed_state = None
        self.desired_state = None
        self.last_action = None
        self.last_action_state = None
        self.last_action_time = None

        self.refresh()

    instance = property(
        lambda self: self.ec2.Instance(self.id) if self.id else None,
        doc="boto3 Instance object belonging to this minion")

    ip = property(lambda self: self.instance.private_ip_address)

    def refresh(self):
        if not self.id:
            self.observed_state = NONEXISTENT
            return
        state = state_from_code(self.instance.state['Code'])
        if state == RUNNING:
            if self.pings():
                state = READY
        self.observed_state = state

    def pings(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((self.ip, 50000))
            s.close()
            return True
        except ConnectionRefusedError:
            return False
        except socket.timeout:
            return False

    def make(self, desired_state):
        assert desired_state in [
            NONEXISTENT,
            PENDING,
            RUNNING,
            SHUTTING_DOWN,
            TERMINATED,
            STOPPING,
            STOPPED,
            READY]
        self.refresh()
        if self.observed_state != desired_state and not self.engine.plan(self.observed_state, desired_state):
            return False
        self.desired_state = desired_state
        return True

    def poll(self):
        self.refresh()

        desired = self.desired_state
        observed = self.observed_state

        if not desired:
            # Starts out as None.
            return

        if observed == desired:
            # We're already there
            return

        action = self.engine.plan(observed, desired)
        if not action:
            print(
                f"Don't know how to get {self.name} from {observed} to {desired}")
            self.desired_state = None
            return

        if action == self.last_action and observed == self.last_action_state:
            # don't retry too often
            if time.time() - self.last_action_time < 60:
                return

        # Ok, do it.
        if action == 'start':
            print(f"* START {self.name}")
            self.instance.start()
        elif action == 'stop':
            print(f"* STOP {self.name}")
            self.instance.stop()
        elif action == 'wait':
            pass
        else:
            assert(False)

        self.last_action = action
        self.last_action_state = observed
        self.last_action_time = time.time()


def track_down_minions(ec2, tags):
    """Look for EC2 instances with certain tags and create Minion instances for them"""

    filters = [
        dict(Name=f'tag:{k}', Values=[v])
        for (k, v) in tags.items()
    ]
    instances = ec2.instances.filter(Filters=filters)
    minions = []
    for instance in ec2.instances.filter(Filters=filters):
        if instance.state['Code'] == TERMINATED.code:
            continue
        id = instance.id
        tags = dict((d['Key'], d['Value']) for d in instance.tags)
        name = tags.get('Name')
        if name == None:
            raise Exception(f"Instance {id} has no Name tag")
        m = Minion(ec2, name, id)
        minions.append(m)
    return sorted(minions, key=lambda m: m.name)

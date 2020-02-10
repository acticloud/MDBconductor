
from collections import defaultdict
import io
import math
import threading
import time
import urllib
import uuid

import boto3
import pymonetdb

import minions
import pool
import teiresias

# actually maybe caller should pass this in but we don't bother
ec2 = boto3.resource('ec2')


class Backend:
    def __init__(self, pools, specs, explainer_connector, minion_connector):
        self._poller_thread = threading.Thread(target=self._polling_loop)
        self._pools = pools
        self._specs = specs
        self._triggers = defaultdict(lambda: 0)
        self._pool_condition = threading.Condition()
        self._pool_condition_sleepers = 0
        self._storage_lock = threading.Lock()
        self._storage_do_not_use_directly = None
        self._explainer_connector = explainer_connector
        self._minion_connector = minion_connector
        self._statushub = PollHub({})
        assert len(pools) > 0
        assert set(specs.keys()) == set(pools.keys())

        self._update_status()
        self._poller_thread.daemon = True
        self._poller_thread.start()

    def _polling_loop(self):
        msgs = dict((name, None) for name in self._pools.keys())
        while 1:
            time.sleep(1)
            wake_them = False
            for name, p in self._pools.items():
                p.poll()
                self._manage_pool_size(p)
                msg = f"Pool {name}: {len(p.members())} members, {p.actual} up, {p.desired} desired, load {p.loadaverage.load:.1f}"
                if msgs[name] != msg:
                    print(msg)
                    wake_them = True
                msgs[name] = msg
            if wake_them:
                #print()
                with self._pool_condition:
                    self._pool_condition.notify_all()
            self._update_status()

    def _manage_pool_size(self, p):
        loadavg = p.loadaverage
        load = loadavg.load
        ups = len(p.classify()['UP'])

        # Ground rule: desired is load, rounded upward.
        new_desired = int(math.ceil(load))
        reason = "load"

        # On start, load==0. We don't want to immediately shut down all minions.
        bottom = max(0, int(ups - math.floor(loadavg.time_running / 60)))
        if new_desired < bottom:
            new_desired = bottom
            reason = "keep some running initially"

        # ceil(load) will never be 0.0 once load has been > 0.
        # This means that new_desired will stay >= 0 forever.
        # At some point we have to shut down the last minion.
        if bottom == 0 and new_desired == 1 and loadavg.time_since_change > 15 * 60 and load < 0.1:
            new_desired = 0
            reason = "no recent activity"

        if new_desired == 0 and self._triggers[p.name] > 0:
            new_desired = 1
            reason = "triggered"

        new_desired = min(new_desired, len(p.members()))
        if new_desired != p.desired:
            print(
                f"Pool {p.name} load {load:.1f} desired {p.desired} -> {new_desired} ({reason})")

        p.desired = new_desired

    def _connector_for_ip(self, ip):
        parsed = self._minion_connector.parsed()
        netloc = parsed.netloc
        if not 'HOSTNAME' in netloc:
            raise Exception(
                "Minion connector template should use literal string 'HOSTNAME'")
        netloc = netloc.replace('HOSTNAME', ip)
        connector = self._minion_connector.override(netloc=netloc)
        return connector

    def status(self, id=None, seen=0):
        return self._statushub.get_state(id, seen)

    def _update_status(self):
        # re-use status report from c2.py
        out = io.StringIO()
        stats = {}
        for pool in self._pools.values():
            may_shrink = ", postponing shrinks" if pool.postpone_shrink else ""
            print(
                f"Pool {pool.name}, load={pool.loadaverage.load:.1f}, actual={pool.actual}, desired={pool.desired}{may_shrink}:",
                file=out)
            for i, (name, state, claims, minion) in enumerate(pool.members()):
                print(f"{i+1:2d} {name} state={state}",
                      file=out, end="")
                observed = minion.observed_state
                desired = minion.desired_state
                if observed == desired:
                    print(f" minion={minion.observed_state}", file=out, end="")
                else:
                    print(
                        f" minion={minion.observed_state}->{minion.desired_state}", file=out, end="")
                print(file=out)
            print(file=out)
            classification=pool.classify()
            stats[pool.name] = dict(
                load=pool.loadaverage.load,
                up=len(classification['UP']),
                starting=len(classification['STARTING']),
                actual=pool.actual,
                desired=pool.desired,
            )
            status = dict(
                stats=stats,
                text=out.getvalue(),
            )
        self._statushub.set_state(status, filter=lambda s:s.get('text'))

    def set_pool_size(self, poolname, size):
        p = self._pools.get(poolname)
        if p:
            print(f"Set desired size of {poolname} to {size}")
            p.desired = size
        else:
            msg = f"Pool {poolname} not found, try one of {', '.join(self._pools.keys())}"
            print(msg)
            raise Exception(msg)

    def wait_for_pool(self, pool):
        with self._pool_condition:
            c = pool.claim()
            if c:
                return c

            if self._pool_condition_sleepers >= 100:
                raise Exception("too busy")
            try:
                self._pool_condition_sleepers += 1
                self._triggers[pool.name] += 1
                print(
                    f"Thread {threading.current_thread().name} waiting for pool {pool.name}")
                while 1:
                    c = pool.claim()
                    if c:
                        print(
                            f"Thread {threading.current_thread().name} proceeding with pool {pool.name}")
                        return c
                    self._pool_condition.wait()
            finally:
                self._pool_condition_sleepers -= 1
                self._triggers[pool.name] -= 1

    def claim_any_pool(self):
        for p in self._pools.values():
            c = p.claim()
            if c:
                return c

        first_pool = list(self._pools.values())[0]
        return self.wait_for_pool(first_pool)

    def get_storage(self):
        with self._storage_lock:
            if not self._storage_do_not_use_directly:
                with self.claim_any_pool() as claim:
                    connector = self._connector_for_ip(claim.ip)
                    conn = connector.connect()
                    try:
                        storage = teiresias.get_storage(conn)
                        self._storage_do_not_use_directly = storage
                        print("Succesfully retrieved storage stats")
                    finally:
                        conn.close()
            return self._storage_do_not_use_directly

    def execute_query(self, q):
        # Not sure if connection is thread safe, better create new one.
        # Future work: connection pool

        # First get some advice
        storage = self.get_storage()
        conn = self._explainer_connector.connect()
        try:
            adviser = teiresias.Adviser(conn, storage)
            adv = adviser.advise(q, self._specs)
        finally:
            conn.close()

        # Then send the query to the recommended pool
        p = self._pools[adv]

        with self.wait_for_pool(p) as claim:
            connector = self._connector_for_ip(claim.ip)
            conn = connector.connect()
            try:
                cursor = conn.cursor()
                rows = cursor.execute(q)
                return dict(
                    query=q,
                    advice=adv,
                    ip=claim.ip,
                    url=connector.url,
                    rows=rows,
                )
            finally:
                conn.close()


INSTANCE_TYPE_MEMORY_MiB = {
    "t2.micro": 1024,
    "t2.small": 2048,
    "t2.medium": 4096,
    "t2.large": 8192,
    "t2.xlarge": 16384,
}


def make_backend(explainer_connector, minion_connector_template, filters):
    pools = {}
    specs = {}
    for name, filter in filters.items():
        ms = minions.track_down_minions(ec2, filter)
        if not ms:
            raise Exception(f"Found no {name} minions using filter {filter}")
        example = ms[0]
        instance_type = example.instance.instance_type
        mem = INSTANCE_TYPE_MEMORY_MiB.get(instance_type)
        if not mem:
            raise Exception(
                f"Don't know how much memory a {instance_type} has")
        p = pool.Pool(name, ms)
        pools[name] = p
        specs[name] = mem * 1024 * 1024 * 1.0

    return Backend(pools, specs, explainer_connector, minion_connector_template)


class Connector:
    def __init__(self, url):
        if not url.startswith('mapi:monetdb:'):
            raise Exception("Expect MAPI url to start with mapi:monetdb:")
        self._url = url
        path = self.parsed().path
        if not path or path == '/':
            raise Exception(f"MAPI URL {url} does not contain a database name")
        if '/' in path[1:]:
            raise Exception(f"Database name must not contain slashes: {url}")

    @property
    def url(self):
        return self._url

    def parsed(self):
        return urllib.parse.urlparse(self.url[5:])

    def override(self, **kwargs):
        parts = self.parsed()
        newparts = parts._replace(**kwargs)
        url = 'mapi:' + urllib.parse.urlunparse(newparts)
        return Connector(url)

    def connect(self):
        parsed = self.parsed()
        # port handling slightly incorrect; no support for unix_socket yet :(
        conn = pymonetdb.connect(
            database=parsed.path[1:] if parsed.path else None,
            hostname=parsed.hostname,
            port=parsed.port or 50000,
            username=parsed.username or 'monetdb',
            password=parsed.password or 'monetdb',
        )
        return conn

    def __call__(self):
        return self.connect()


class PollHub:
    def __init__(self, initial_state):
        self._last_update = 0
        self._id = uuid.uuid4().hex
        self._condition = threading.Condition()
        self._generation = 1
        self._state = initial_state

    def set_state(self, new_state, filter=lambda x:x):
        now = time.time()
        with self._condition:
            if now - self._last_update < 60 and filter(new_state) == filter(self._state):
                return
            self._state = new_state
            self._last_update = now
            self._generation += 1
            self._condition.notify_all()

    def get_state(self, id=None, seen=0):
        with self._condition:
            if seen > self._generation:
                # impossible!
                seen = self._generation
            while True:
                if id != self._id:
                    break
                if seen < self._generation:
                    break
                self._condition.wait()
            return self._id, self._generation, self._state

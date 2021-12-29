'''
Replicated Concurrency Control and Recovery (RepCRec) System
Class: Advanced Database Systems, CSCI-GA.2434-001, New York University, Fall 2021
Authors: 
1. Narmada Bhagat (nb3136@nyu.edu)
2. Amey Mhadgut (arm994@nyu.edu)
'''
import collections
import enum
import re
import sys
import argparse

def print_(str_):
    if verbose:
        print(str_)

class MyRandom(object):
    R_LIST = [6,0,8,1,9,2,3,5,4,7]  # Deterministic sequence.
    @classmethod
    def rotate(cls):
        MyRandom.R_LIST = MyRandom.R_LIST[1:] + [MyRandom.R_LIST[0]]
    @classmethod
    def randint(cls, a, b):
        MyRandom.rotate()
        return a + MyRandom.R_LIST[0] % (b - a + 1)
        
class VariableValue(collections.namedtuple('VariableValue', ['tx_id', 'val'])):
    pass

# Data
class DataVariable(object):
    '''
    Class to store data variables
    '''
    def __init__(self, index, val, replica):
        '''
        Method to initialize the DataVariable
        '''
        self.latest_val = val # Variable Value
        self.latest_committed_val = val  # Value of variable that when it was last committed. Can't be more than 1 due to 2PL.
        self.latest_uncommited_tx = None
        self.valid_for_read = True
        self.history = [VariableValue(-1, val)]  # variable value over time. tx_id of -1 indicate initial value.
        self.index = index  # Variable Index
        self.replica = replica # Which site contains this variable.
    
    @property
    def name(self):
        return 'x%d' % self.index
    
    @property
    def val(self):
        return self.latest_val

    @val.setter
    def val(self, value):
        self.latest_val = value

    def commit(self, tx_id):
        self.latest_committed_val = self.latest_val
        self.latest_uncommited_tx = None
        self.valid_for_read = True

    def abort(self, tx_id):
        if self.latest_uncommited_tx == tx_id:
            self.val = self.latest_committed_val
            self.latest_uncommited_tx = None
            self.history = [v for v in self.history if v.tx_id != tx_id]
            # valid_for_read is unchanged.

    @property
    def fullname(self):
        return 'x%d.%d' % (self.index, self.replica)
	
    @property 
    def is_replicated(self): 
        return self.index % 2 == 0  # If the variable is replicated or not.

    def update(self, tx_id, new_value):
        '''Updates value of variable and add the value to history.'''
        self.val = new_value
        self.latest_uncommited_tx = tx_id
        self.history.append(VariableValue(tx_id, new_value))

    def __str__(self):
        return '%s: %d' % (self.fullname, self.val)


'''
The data consists of 20 distinct variables x1, ..., x20 (the numbers between 1 and 20 will be referred to as indexes below).
There are 10 sites numbered 1 to 10. A copy is indicated by a dot. Thus, x6.2 is the copy of
variable x6 at site 2. The odd indexed variables are at one site each (i.e. 1
+ (index number mod 10) ). For example, x3 and x13 are both at site 4.
Even indexed variables are at all sites.
Each variable xi is initialized to the value 10i (10 times i). 
'''

class Lock(enum.Enum):
    UNLOCKED = 0
    READ = 1
    WRITE = 2

class SiteStatus(enum.Enum):
    UP = 1
    DOWN = 0

class LockContext(object):
    def __init__(self, tx_id, lock):
      self.tx_id = tx_id
      self.lock = lock

class LockData(object):
    def __init__(self, state, wait_queue, tx_ids_holding_lock):
        self.state = state
        self.wait_queue = wait_queue
        self.tx_ids_holding_lock = tx_ids_holding_lock

'''
At each variable locks are acquired in a first-come first-serve fashion, e.g.
if requests arrive in the order R(T1,x), R(T2,x), W(T3,x,73), R(T4,x) then,
assuming no transaction aborts, first T1 and T2 will share read locks on x,
then T3 will obtain a write lock on x and then T4 a read lock on x. Note
that T4 doesn’t skip in front of T3, as that might result in starvation for
writes. Note also that the blocking (waits-for) graph will have edges from
T3 to T2 from T3 to T1 and from T4 to T3.
In the case of requests arriving in the order R(T1,x), R(T2,x), W(T1,x,73),
there will be a waits-for edge from T1 to T2 because T1 is attempting to
promote its lock from read to write, but cannot do so until T2 releases its
lock.
By contrast, in the case W(T1,x,15), R(T2,x), R(T1,x), there will be a
waits-for edge from T2 to T1 but the R(T1,x) will not need to wait for a
lock because T1 already has a write-lock which is sufficient for the read. The
same would hold in this case: W(T1,x,15), R(T2,x), R(T1,x), W(T1,x,41).
Detect deadlocks using cycle detection and abort the youngest transaction in the cycle.
This implies that your system must keep track of the
transaction time of any transaction holding a lock. (We will ensure that no
two transactions will have the same age.) There is never a need to restart
an aborted transaction. That is the job of the application (and is often done
incorrectly). Deadlock detection need not happen at every tick. When it
does, it should happen at the beginning of the tick.
'''

class LockManager(object):
    def __init__(self, site_index, data):
        self.site_index = site_index
        # Site local lock manager stores the status of lock on each variable at the site
        # For each variable, it store the lock status and if locked the transaction id of
        # transaction holding the lock and the FCFS queue of transaction ids waiting for lock.
        self.lock_status = {v.fullname : LockData(state=Lock.UNLOCKED,
                                                  wait_queue=[],
                                                  tx_ids_holding_lock=set())
                            for v in data}

    def _enqueue_waiting(self, lock_data, tx_id, lock):
        # Enqueue tx_id waiting for lock if not already waiting.
        if tx_id in lock_data.tx_ids_holding_lock and lock_data.state == lock:
            # Already has the lock. Nothing to enqueue.
            return
        for lc in lock_data.wait_queue:
            if lc.tx_id == tx_id and (lc.lock == Lock.WRITE or lc.lock == lock):
                # Tx is already requesting the lock or a write lock. Nothing to enqueue.
                return
        # Populate wait_for edges.
        tx = TX_MAP[tx_id]
        if lock_data.state == Lock.WRITE or lock == Lock.WRITE:
            tx.waits_for.update(set(
                lock_holding_tx for lock_holding_tx in lock_data.tx_ids_holding_lock
                if lock_holding_tx != tx_id))
        for lc in lock_data.wait_queue:
            if (lc.lock == Lock.WRITE or lock == Lock.WRITE) and lc.tx_id != tx_id:
                tx.waits_for.add(lc.tx_id)
        # print ('{0} Waiting for {1}'.format(tx_id, tx.waits_for))
        lock_data.wait_queue.append(LockContext(tx_id, lock))

    # If an operation for T1 is waiting for a lock held by T2, then when T2
    # commits, the operation for T1 proceeds if there is no other lock request ahead
    # of it. Lock acquisition is first come first served but several transactions may
    # hold read locks on the same item. As illustrated above, if x is currently
    # read-locked by T1 and there is no waiting list and T2 requests a read lock
    # on x, then T2 can get it. However, if x is currently read-locked by T1 and
    # T3 is waiting for a write lock on x and T2 subsequently requests a read
    # lock on x, then T2 must wait for T3 either to be aborted or to complete its
    # possesion of x; that is, T2 cannot skip T3.
    def read_lock_acquire_or_enqueue(self, var_fullname, tx_id):
        # print ('{0} attempting to acquire read lock on {1}'.format(tx_id, var_fullname))
        lock_data = self.lock_status[var_fullname]
        if lock_data.state == Lock.UNLOCKED: # Not locked, so acquire
            lock_data.state = Lock.READ
            lock_data.tx_ids_holding_lock.add(tx_id)
            return True
        elif tx_id in lock_data.tx_ids_holding_lock:  # Already holding lock
            # do nothin.
            return True 
        elif lock_data.state == Lock.READ:  # No lock, or read
            for waiters in lock_data.wait_queue:
                if waiters.lock == Lock.WRITE:
                    self._enqueue_waiting(lock_data, tx_id, Lock.READ)
                    return False
            lock_data.state = Lock.READ
            lock_data.tx_ids_holding_lock.add(tx_id)
            return True  # Lock acquired.
        else:  # Write lock on the variable, wait for it to complete.
            self._enqueue_waiting(lock_data,tx_id, Lock.READ)
            return False  # Lock not acquired, waiting.

    def write_lock_try_acquire(self, var_fullname, tx_id):
        '''Check if tx can acquire write lock.'''
        # print ('{0} attempting to acquire write lock on {1}'.format(tx_id, var_fullname))
        lock_data = self.lock_status[var_fullname]
        if lock_data.state == Lock.UNLOCKED:  # No lock
            return True  # Lock acquired.
        elif lock_data.state == Lock.WRITE and tx_id in lock_data.tx_ids_holding_lock:
            # Already holding write lock. Do nothing.
            return True
        elif (lock_data.state == Lock.READ and
              tx_id in lock_data.tx_ids_holding_lock and 
              len(lock_data.tx_ids_holding_lock) == 1):
            # Holding read lock and is the only execution holding read lock.
            # Promote to write lock if no other writers waiting.
            return len([lc for lc in lock_data.wait_queue if lc.lock == Lock.WRITE]) == 0
        else:  # Write lock on the variable, wait for it to complete.
            return False  # Lock not acquired, waiting.

    def write_lock_acquire_or_enqueue(self, var_fullname, tx_id):
        lock_data = self.lock_status[var_fullname]
        if lock_data.state == Lock.UNLOCKED:  # No lock
            lock_data.state = Lock.WRITE
            lock_data.tx_ids_holding_lock.add(tx_id)
            return True  # Lock acquired.
        elif lock_data.state == Lock.WRITE and tx_id in lock_data.tx_ids_holding_lock:
            # Already holding write lock. Do nothing.
            return True
        elif lock_data.state == Lock.READ and tx_id in lock_data.tx_ids_holding_lock and len(lock_data.tx_ids_holding_lock) == 1:
            # Holding read lock and is the only execution holding read lock.
            # Promote to write lock if no other write lock is pending.
            if len([lc for lc in lock_data.wait_queue if lc.lock == Lock.WRITE]) == 0:
                lock_data.state = Lock.WRITE
                return True
            else:
                self._enqueue_waiting(lock_data, tx_id, Lock.WRITE)
                return False  # Lock not acquired, waiting.    
        else:  # Write lock on the variable, wait for it to complete.
            self._enqueue_waiting(lock_data, tx_id, Lock.WRITE)
            return False  # Lock not acquired, waiting.
    
    def write_lock_release(self, var_fullname, tx_id):
        lock_data = self.lock_status[var_fullname]
        if lock_data.state == Lock.UNLOCKED:  # No Lock. Do nothing.
            return
        if lock_data.state == Lock.WRITE and tx_id in lock_data.tx_ids_holding_lock:
            lock_data.state = Lock.UNLOCKED
            lock_data.tx_ids_holding_lock.remove(tx_id)
        else:
            # Remove the write lock from the wait queue. (And attempt to unblock subsequent read)
            # Note that lock was never granted in this case. So, the write has been aborted.
            write_lock_request = [i for i, v in enumerate(lock_data.wait_queue)
                                  if v.tx_id == tx_id and v.lock == Lock.WRITE]
            if len(write_lock_request) == 0:
                # Lock not waiting in queue, do nothing
                return
            for i in write_lock_request:
                lock_data.wait_queue.pop(i)
        # All blocked transaction will try to acquire lock at the beginning of next tick.
    
    def read_lock_release(self, var_fullname, tx_id):
        lock_data = self.lock_status[var_fullname]
        if lock_data.state == Lock.UNLOCKED:  # No Lock. Do nothing.
            return
        if lock_data.state == Lock.READ and tx_id in lock_data.tx_ids_holding_lock:
            lock_data.tx_ids_holding_lock.remove(tx_id)
            if len(lock_data.tx_ids_holding_lock) == 0:
                lock_data.state = Lock.UNLOCKED
        else:
            # Remove the read lock from the wait queue.
            # Note that lock was never granted in this case. So, the read has been aborted.
            read_lock_request = [i for i, v in enumerate(lock_data.wait_queue)
                                  if v.tx_id == tx_id and v.lock == Lock.READ]
            if len(read_lock_request) == 0:
                # Lock not waiting in queue, do nothing
                return
            for i in read_lock_request:
                lock_data.wait_queue.pop(i)
        # All blocked transaction will try to acquire lock at the beginning of next tick.


class ExecStatus(enum.Enum):
    SITE_DOWN = 0
    READ_SUCCESS = 1
    READ_WAITING = 2
    WRITE_SUCCESS = 3
    WRITE_WAITING = 4
    INVALID = 5
MAX_UPTIME = 2**31

class DataManager(object):

    def __init__(self, site_index, status=SiteStatus.UP, site_uptime=0):
        self.site_index = site_index
        self.data = {v.fullname: v for rv in VARS for v in rv if v.replica == self.site_index}
        self.status = status # var to store current status of the DM/site
        self.site_uptime = site_uptime
        # Each site has an independent lock table. If that site fails, the lock table is erased.
        self.read_locks_requested_map = collections.defaultdict(set)  # For efficiency store which locks are read and write.
        self.write_locks_requested_map = collections.defaultdict(set)
        self.locktable = LockManager(self.site_index, self.data.values())

    def fail(self):
        '''Fails this site. (data will persist however locks are released).'''
        self.status = SiteStatus.DOWN
        self.site_uptime = MAX_UPTIME
        # Invalidate all lock structures held here.
        self.read_locks_requested_map = None
        self.write_locks_requested_map = None
        self.locktable = None
        for var in self.data.values():
            var.valid_for_read = False
            # We can call abort here. However it doesn't matter. We will not call it.

    def recover(self, time):
        '''Brings this site up.'''
        self.status = SiteStatus.UP
        self.site_uptime = time
        # Recreate lock structures held locally at site.
        self.read_locks_requested_map = collections.defaultdict(set)
        self.write_locks_requested_map = collections.defaultdict(set)
        self.locktable = LockManager(self.site_index, self.data.values())
        for _, v in self.data.items():
            # Upon recovery of a site s, all non-replicated
            # variables are available for reads and writes. Regarding replicated variables,
            # the site makes them available for writing, but not reading. In fact, a read
            # for a replicated variable x will not be allowed at a recovered site until a
            # committed write to x takes place on that site (see lecture notes on recovery
            # when using the available copies algorithm).
            if not v.is_replicated:
                # Make variable read only on recover if not replicated.
                v.valid_for_read = True
            else:
                # Replicated variable can't be read unless written.
                v.valid_for_read = False

    def read_read_only(self, tx_id, var_name):
        '''Reads variable from a read-only Tx.'''
        if self.status == SiteStatus.DOWN:
            return ExecStatus.SITE_DOWN, None
        
        var_fullname = '%s.%s' % (var_name, self.site_index)
        variable_data = self.data[var_fullname]
        tx_start_time = TX_MAP[tx_id].start_time
        latest_value = None
        latest_committed_ts = -1
        for historical_tx_id, value in variable_data.history:
            # Look for value committed before the start of transaction.
            if historical_tx_id == -1:
                latest_value = value
            elif (TX_MAP[historical_tx_id].status == TxStatus.COMMITTED
                  and TX_MAP[historical_tx_id].commit_time > latest_committed_ts
                  and TX_MAP[historical_tx_id].commit_time < tx_start_time):
                latest_value = value
                latest_committed_ts = TX_MAP[historical_tx_id].commit_time
        if latest_value:
            return ExecStatus.READ_SUCCESS, latest_value
        else:
            return ExecStatus.READ_WAITING, None
            # TODO: Handling abort for transaction when RO transaction waits.

    # Handle read for non read only transaction.
    def read(self, tx_id, var_name):
        '''Reads the variable if possible (site is up and variable is not locked).'''
        # Read from one copy; On read, if a client cannot read a copy of x
        # from a server (e.g. if client times out ), then
        # read x from another server (will be implemented on TM side).
        
        # Check if the site is up.
        var_fullname = '%s.%d' % (var_name, self.site_index)
        if self.status == SiteStatus.DOWN or not self.data[var_fullname].valid_for_read:
            return ExecStatus.SITE_DOWN, None

        # Check if this variable can be read at this site.
        if var_fullname not in self.data:
            return ExecStatus.INVALID, None
        # Check if lock can be acquired.
        self.read_locks_requested_map[tx_id].add(var_fullname)
        if self.locktable.read_lock_acquire_or_enqueue(var_fullname, tx_id):
            return ExecStatus.READ_SUCCESS, self.data[var_fullname].val
        else:
            return ExecStatus.READ_WAITING, None
    
    # TODO: For each function below, verify that site is up. Ideally we should have a decorator @ensure_up
    # that checks this (except for recover).
    def can_write(self, var_name, tx_id):
        '''Check variable update is possible at this site (site is up and variable is not locked).'''
        # Verify that writing is allowed.
        # Check if this variable can be read at this site.
        var_fullname = '%s.%d' % (var_name, self.site_index)
        if var_fullname not in self.data:
            return ExecStatus.INVALID
        # Check if the site is up.
        if self.status == SiteStatus.DOWN:
            return ExecStatus.SITE_DOWN
        if not self.locktable.write_lock_try_acquire(var_fullname, tx_id):
            return ExecStatus.WRITE_WAITING
        return ExecStatus.WRITE_SUCCESS

    def read_lock(self, var_name, tx_id):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        var_fullname = '%s.%d' % (var_name, self.site_index)
        self.read_locks_requested_map[tx_id].add(var_fullname)
        if not self.locktable.read_lock_acquire_or_enqueue(var_fullname, tx_id):
            raise Exception('Trying to take read lock when access not permitted')

    def update(self, var_name, tx_id, value):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        var_fullname = '%s.%d' % (var_name, self.site_index)
        self.write_locks_requested_map[tx_id].add(var_fullname)
        if not self.locktable.write_lock_acquire_or_enqueue(var_fullname, tx_id):
            raise Exception('Trying update when lock cant be aquired')
        self.data[var_fullname].update(tx_id, value)  # Finish write.
    
    def commit(self, tx_id):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        # If site goes down, this variable write will not be committed as the locks are cleared.
        if not tx_id in self.write_locks_requested_map:
            return
        for var_fullname in self.write_locks_requested_map[tx_id]:
            self.data[var_fullname].commit(tx_id)

    def abort(self, tx_id):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        
        # if tx_id not in self.write_locks_requested_map:
        #     return
        # for var_fullname in self.write_locks_requested_map[tx_id]:

        for var_fullname in VARS_MAP:
            if var_fullname in self.data:
                self.data[var_fullname].abort(tx_id)

    def enqueue_write(self, var_name, tx_id):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        var_fullname = '%s.%d' % (var_name, self.site_index)
        self.write_locks_requested_map[tx_id].add(var_fullname)
        # Enqueue tx_id in list of transaction waiting to acquire lock on var_name.
        # We will prevent any other instruction starting after tx_id
        # taking lock on this variable to access this variable.
        self.locktable.write_lock_acquire_or_enqueue(var_fullname, tx_id)
    
    def lock_release_phase(self, tx_id):
        if self.status == SiteStatus.DOWN:
            # TODO: Send error.
            return
        if tx_id in self.read_locks_requested_map:
            for var_fullname in self.read_locks_requested_map[tx_id]:
                self.locktable.read_lock_release(var_fullname, tx_id)
            del self.read_locks_requested_map[tx_id]
        if tx_id in self.write_locks_requested_map:
            for var_fullname in self.write_locks_requested_map[tx_id]:
                self.locktable.write_lock_release(var_fullname, tx_id)
            del self.write_locks_requested_map[tx_id]

'''
Algorithms to use
Please implement the available copies approach to replication using strict
two phase locking (using read and write locks) at each site and validation at
commit time. A transaction may read a variable and later write that same
variable as well as others. Please use the version of the algorithm specified
in my notes rather than in the textbook, just for consistency. Note that
available copies allows writes and commits to just the available sites, so if
site A is down, its last committed value of x may be different from site B
which is up.
Read-only transactions should use multiversion read consistency. Here
are some implementation notes that may be helpful. In which situations can
a read-only transaction read RO an item xi?
1. If xi is not replicated and the site holding xi is up, then the read-only
transaction can read it. Because that is the only site that knows about
xi.
2. If xi is replicated then RO can read xi from site s if xi was committed
at s by some transaction T’ before RO began and s was up all the time
between the time when xi was commited and RO began. In that case
RO can read the version that T’ wrote. If there is no such site then
RO can abort.
Implementation: for every version of xi, on each site s, record when that
version was committed. Also, the TM will record the failure history of every
site.
Examples:
• Sites 2-10 are down at 12 noon Site 1 commits a transaction T that
writes x2 at 1 PM At 1:30 pm a read-only transaction begins. Site
1 has been up since transaction T commits its update to x2 and the
beginning of the read-only transaction. No other transaction wrote x2
in the meantime. Site 1 fails here at 2 PM. The read-only transaction
can still use the values it had from site 1.
• Sites 2-10 are down at 12 noon Site 1 commits a transaction that writes
x2 at 1 PM Site 1 fails here At 2 pm a read-only transaction begins.
It can abort because no site has been up since a transaction commits
its update to x2 and the beginning of the read-only transaction.

Test Specification
When we test your software, input instructions come from a file or the
standard input, output goes to standard out. (That means your algorithms
may not look ahead in the input.) Each line will have at most a single
instruction from one transaction or a fail, recover, dump, end, etc. Some
of these operations may be blocked due to conflicting locks. We will ensure
that when a transaction is waiting, it will not receive another operation.


The execution file has the following format:
begin(T1) says that T1 begins
beginRO(T3) says that T3 begins and is read-only
R(T1, x4) says transaction 1 wishes to read x4 (provided it can get the
locks or provided it doesn’t need the locks (if T1 is a read-only transaction)).
It should read any up (i.e. alive) copy and return the current value (or the
value when T1 started for read-only transaction). It should print that value
in the format
x4: 5
on one line by itself.
W(T1, x6,v) says transaction 1 wishes to write all available copies of x6
(provided it can get the locks on available copies) with the value v. So, T1
can write to x6 only when T1 has locks on all sites that are up and that
contain x6. If a write from T1 can get some locks but not all, then it is an
implementation option whether T1 should release the locks it has or not.
However, for purposes of clarity we will say that T1 should release those
locks.
dump() prints out the committed values of all copies of all variables at
all sites, sorted per site with all values per site in ascending order by variable
name, e.g.
site 1 – x2: 6, x3: 2, ... x20: 3
...
site 10 – x2: 14, .... x8: 12, x9: 7, ... x20: 3
This includes sites that are down (which of course should not reflect writes
when they are down).
in one line.
one line per site.
end(T1) causes your system to report whether T1 can commit in the
format
T1 commits
or
T1 aborts
fail(6) says site 6 fails. (This is not issued by a transaction, but is just
an event that the tester will execute.)
recover(7) says site 7 recovers. (Again, a tester-caused event) We discuss
this further below.
A newline in the input means time advances by one. There will be one
instruction per line.
Other events to be printed out: (i) when a transaction commits, the
name of the transaction; (ii) when a transaction aborts, the name of the
transaction; (iii) which sites are affected by a write (based on which sites
are up); (iv) every time a transaction waits because of a lock conflict (v)
every time a transaction waits because a site is down (e.g., waiting for an
unreplicated item on a failed site).
Example (partial script with six steps in which transactions T1 commits,
and one of T3 and T4 may commit)
begin(T1)
begin(T2)
begin(T3)
W(T1, x1,5)
W(T3, x2,32)
W(T2, x1,17) // will cause T2 to wait, but the write will go ahead after T1
commits
end(T1)
begin(T4)
W(T4, x4,35)
W(T3, x5,21)
W(T4,x2,21)
W(T3,x4,23) // T4 will abort because it’s younger
Design
Your program should consist of two parts: a single transaction manager
that translates read and write requests on variables to read and write requests on
copies using the available copy algorithm described in the notes.
The transaction manager never fails. (Having a single global transaction
manager that never fails is a simplification of reality, but it is not too hard
to get rid of that assumption by using a shared disk configuration. For this
project, the transaction manager is also fulfilling the role of a broker which
routes requests and knows the up/down status of each site.)
'''

class TxType(enum.Enum):
    READ_ONLY = 0
    TWO_PL = 1

class TxStatus(enum.Enum):
    RUNNING = 0
    COMMITTED = 1
    ABORTED = 2

class Transaction(object):
    def __init__(self, tx_id, start_time, type=TxType.TWO_PL):
        self.tx_id = tx_id
        self.type = type
        self.start_time = start_time
        self.commit_time = None
        self.status = TxStatus.RUNNING
        self.waits_for = set()
        if self.tx_id not in TX_MAP:
            TX_MAP[self.tx_id] = self
        else:
            raise Exception('Duplicate transaction id')
        self.sites_accessed = {}

class Instruction(collections.namedtuple('Instruction', ['tx_id', 'start_time', 'instruction', 'var_name', 'value'])):
    def __repr__(self):
        return '{0}({1},{2},{3})'.format(self.instruction, self.tx_id, self.var_name, self.value)
    def __eq__(self, other):
        return self.start_time == other.start_time  # No two instruction share same start time.

class TransactionManager(object):
    def __init__(self, site_count):
        '''Create Transaction manager'''
        self.site_count = site_count
        self.sites = [DataManager(i) for i in range(1, 1 + site_count)]  # Site managers initialized.

        self.var_mapping = collections.defaultdict(list)
        for site in self.sites:
            for var_name in site.data:
              self.var_mapping[var_name.split('.')[0]].append(site)
        self.tx_waiting = collections.defaultdict(list)
        self.instruction_waiting = []
        self.try_pending_instructions = False

    def dfs(self, root, visited, path, abort_tx):
        if visited[root] != 0:
            return
        visited[root] = 1
        
        for tx_id in TX_MAP[root].waits_for:
            if visited[tx_id] == 0:
                path.append(tx_id)
                self.dfs(tx_id, visited, path, abort_tx)
                path.pop()
            if visited[tx_id] == 1:
                # Found cycle. Terminating the youngest transaction.
                # Only gray edge (dfs not completed) indicate cycle in directed graph.
                # tx_id must be in the path. So, check for all the txs from tx_id to root
                # and pick a transaction with smallest start_time.
                youngest_tx = tx_id
                youngest_tx_ts = TX_MAP[tx_id].start_time
                for tx_in_cycle in reversed(path):
                    if tx_in_cycle == tx_id:
                        break
                    if TX_MAP[tx_in_cycle].start_time > youngest_tx_ts:
                        youngest_tx_ts = TX_MAP[tx_in_cycle].start_time
                        youngest_tx = tx_in_cycle
                abort_tx.add(youngest_tx)                

        visited[root] = 2

    def _detect_cycle_and_abort_tx(self, time):
        visited = {tx_id: 0 for tx_id in TX_MAP if TX_MAP[tx_id].status == TxStatus.RUNNING}
        abort_tx = set()
        for tx_id in TX_MAP:
            if TX_MAP[tx_id].status == TxStatus.RUNNING:
                self.dfs(tx_id, visited, [tx_id], abort_tx)
        if len(abort_tx) == 0:
            return
        else:
            # Select the youngest tx from the options and run the DFS again.
            # This will ensure minimum number of aborts as there may be more than 1 cycle added.
            youngest_tx = ''
            youngest_tx_ts = -1
            for tx_id in abort_tx:
                if TX_MAP[tx_id].start_time > youngest_tx_ts:
                    youngest_tx_ts = TX_MAP[tx_id].start_time
                    youngest_tx = tx_id
            # Abort youngest_tx
            TX_MAP[youngest_tx].status = TxStatus.ABORTED
            print_('{2}> Detected cycle. Aborting {0} which started on {1}'.format(youngest_tx, youngest_tx_ts, time))
            self._try_commit(youngest_tx, time)



    def _try_pending(self, start_time):
        self._detect_cycle_and_abort_tx(start_time)
        while(self.try_pending_instructions):
            # TODO (optional): We are detecting cycle for every instruction when we are also
            # looking for any pending instructions that is unblocked. We can do these independently.
            # However that will add randmness to the implementation. So, we are doing it with every tick.
            
            tx_waiting = set()
            self.try_pending_instructions = False
            executed = []
            for idx, instr in enumerate(self.instruction_waiting):
                if instr.tx_id in tx_waiting:
                    # Once and instruction from a Tx is blocked. Stop further instrutions.
                    continue
                if instr.instruction == 'R':
                    if not self._read(instr.tx_id, instr.var_name, start_time):
                        tx_waiting.add(instr.tx_id)
                    else:
                        self.tx_waiting[instr.tx_id].pop(0)  # Already time ordered.
                        executed.append(idx)
                elif instr.instruction == 'W':
                    if not self._write(instr.tx_id, instr.var_name, instr.value, start_time):
                        tx_waiting.add(instr.tx_id)
                    else:
                        self.tx_waiting[instr.tx_id].pop(0)  # Already time ordered.
                        executed.append(idx)
                elif instr.instruction == 'E':
                    self._try_commit(instr.tx_id, start_time)  # Commit don't go pending.
                    if instr.tx_id in self.tx_waiting:
                        # Should never reach but just in case.
                        self.tx_waiting[instr.tx_id].pop(0)
                    executed.append(idx)
                # Clean waiting instructon if empty.
                if len(self.tx_waiting[instr.tx_id]) == 0:
                    del self.tx_waiting[instr.tx_id]
            # Remove executed instructions from pending.
            self.instruction_waiting = [v for i, v in enumerate(self.instruction_waiting) if i not in executed]
            self._detect_cycle_and_abort_tx(start_time)


    def begin_tx(self, tx_id, start_time):
        return Transaction(tx_id, start_time)
    
    def begin_tx_read_only(self, tx_id, start_time):
        return Transaction(tx_id, start_time, TxType.READ_ONLY)

    def _get_site(self, site_index):
        return self.sites[site_index - 1]

    def _random_permute_sites(self, sites):
        for i in range(len(sites)):
            tmp = sites[i]
            rand_index = MyRandom.randint(i, len(sites) - 1)
            sites[i] = sites[rand_index]
            sites[rand_index] = tmp
        return sites

    def _add_pending(self, tx_id, instr):
        self.tx_waiting[tx_id].append(instr)
        self.instruction_waiting.append(instr)
    # R(T1, x4) says transaction 1 wishes to read x4 (provided it can get the
    # locks or provided it doesn’t need the locks (if T1 is a read-only transaction)).
    # It should read any up (i.e. alive) copy and return the current value (or the
    # value when T1 started for read-only transaction). It should print that value
    # in the format
    # x4: 5
    # on one line by itself.
    def available_sites(self, var_name):
        return list(filter(lambda dm: dm.status == SiteStatus.UP, self.var_mapping[var_name]))

    def read(self, tx_id, var_name, start_time):
        self._try_pending(start_time)
        # If the transaction is waiting then accumate its instruction for executing later.
        if tx_id in self.tx_waiting and len(self.tx_waiting[tx_id]) > 0:
            self._add_pending(tx_id, Instruction(tx_id, start_time, 'R', var_name, value=None))
            return
        if not self._read(tx_id, var_name, start_time):
            # No available sites found.
            self._add_pending(tx_id, Instruction(tx_id, start_time, 'R', var_name, value=None))
            if TX_MAP[tx_id].type == TxType.READ_ONLY:
                print_ ('{2}> {0}: {1} Read waiting. No valid site to read'.format(tx_id, var_name, start_time))
            else:
                print_('{3}> {0}: {1} Read waiting for {2}'.format(
                    tx_id, var_name,
                    ','.join(TX_MAP[tx_id].waits_for) if TX_MAP[tx_id].waits_for else 'no available site',
                    start_time))

    def _read(self, tx_id, var_name, start_time):
        tx = TX_MAP[tx_id]
        for data_manager in self._random_permute_sites(self.available_sites(var_name)):
            if (tx.type == TxType.READ_ONLY):
                err, value = data_manager.read_read_only(tx_id, var_name)
            else:
                err, value = data_manager.read(tx_id, var_name)
            if err == ExecStatus.READ_SUCCESS:
                if data_manager.site_index not in tx.sites_accessed:
                    tx.sites_accessed[data_manager.site_index] = start_time
                else:
                    tx.sites_accessed[data_manager.site_index] = min(
                        start_time, tx.sites_accessed[data_manager.site_index])
                print_('{2}> {0}: {1}'.format(var_name, value, start_time))
                print_('DEBUG: From site %d' % data_manager.site_index)
                # Hold read lock on all sites on success.
                if tx.type == TxType.TWO_PL:
                    # Take read lock on all available sites
                    # TODO(Q): What to do if a new site comes up. Should the read lock be taken.
                    for site in self.available_sites(var_name):
                        site.read_lock(var_name, tx_id)
                return True
        return False
    
    def write(self, tx_id, var_name, value, start_time):
        # W(T1, x6,v) says transaction 1 wishes to write all available copies of x6
        # (provided it can get the locks on available copies) with the value v. So, T1
        # can write to x6 only when T1 has locks on all sites that are up and that
        # contain x6. If a write from T1 can get some locks but not all, then it is an
        # implementation option whether T1 should release the locks it has or not.
        # However, for purposes of clarity we will say that T1 should release those
        # locks.
        # If the transaction is waiting then accumate its instruction for executing later.
        self._try_pending(start_time)
        if TX_MAP[tx_id].type == TxType.READ_ONLY:
            raise Exception('Trying to write from a ReadOnly tx')
        if tx_id in self.tx_waiting and len(self.tx_waiting[tx_id]) > 0:
            self._add_pending(tx_id, Instruction(tx_id, start_time, 'W', var_name, value))
            print_('{3}> {0}: {1} Write waiting for {2}'.format(tx_id, var_name,
                ','.join(TX_MAP[tx_id].waits_for) if TX_MAP[tx_id].waits_for else 'no available site',
                start_time))
            return
        if not self._write(tx_id, var_name, value, start_time):
            for site in self.available_sites(var_name):
                site.enqueue_write(var_name, tx_id)
            self._add_pending(tx_id, Instruction(tx_id, start_time, 'W', var_name, value))
            # Release acquired locks. Nothing needs to be done as lock is acquired only during actual update.
            print_('{3}> {0}: {1} Write waiting for {2}'.format(tx_id, var_name,
                ','.join(TX_MAP[tx_id].waits_for) if TX_MAP[tx_id].waits_for else 'no available site',
                start_time))

    def _write(self, tx_id, var_name, value, start_time):
        tx = TX_MAP[tx_id]
        write_blocked = False
        num_sites_for_write = 0
        for data_manager in self.available_sites(var_name):
            write_status = data_manager.can_write(var_name, tx_id)
            if write_status == ExecStatus.WRITE_WAITING:
                write_blocked = True
            if write_status == ExecStatus.WRITE_SUCCESS:
                num_sites_for_write = num_sites_for_write + 1
        if not write_blocked and num_sites_for_write > 0:
            for site in self.available_sites(var_name):
                site.update(var_name, tx_id, value)
                if site.site_index not in tx.sites_accessed:
                    tx.sites_accessed[site.site_index] = start_time
                else:
                    tx.sites_accessed[site.site_index] = min(
                        start_time, tx.sites_accessed[site.site_index])
            print_('{2}> {0} updated to {1} at sites - {3}'.format(var_name, value, start_time,
                ','.join([str(s.site_index) for s in self.available_sites(var_name)])))
            return True
        else:
            print_ ('Write waiting')
            return False

    def try_commit(self, tx_id, start_time):
        self._try_pending(start_time)
        if tx_id in self.tx_waiting and len(self.tx_waiting[tx_id]) > 0:
            self._add_pending(tx_id, Instruction(tx_id, start_time, 'E', None, None))
            print_ ('{0} Commit Waiting.'.format(tx_id))
            return
        self._try_commit(tx_id, start_time)
    
    def _try_commit(self, tx_id, start_time):
        # At Commit time: For two phase locked transactions, ensure that all servers that
        # you accessed (read or write) have been up since the first time they were accessed.
        # (Note: If the access was a write, this is obvious, since the write might be lost and it
        # would a lot of overhead to check for replicates and whether they have been written
        # to.) Otherwise, abort. (Note: Read-only transactions need not abort in this case.)
        tx = TX_MAP[tx_id]
        if tx.type == TxType.TWO_PL:
            for site_index in tx.sites_accessed:
                if self._get_site(site_index).site_uptime > tx.sites_accessed[site_index]:
                    # Site has failed after request to access it was made.
                    tx.status = TxStatus.ABORTED
        
        if tx.status == TxStatus.ABORTED:
            self.instruction_waiting = [instr for instr in self.instruction_waiting if instr.tx_id != tx_id]
            if tx_id in self.tx_waiting:
                del self.tx_waiting[tx_id]
            print_('{1}> {0} Aborted'.format(tx_id, start_time))
            # Undo any uncommitted writes. We will call abort on all accessed site (including read, however they are no-op)
            for accessed_site in tx.sites_accessed:
                self._get_site(accessed_site).abort(tx_id)
        else:
            if tx_id in self.tx_waiting:
                if len(self.tx_waiting[tx_id]) > 0 and not self.tx_waiting[tx_id][0].instruction == 'E':
                    raise Exception('Intructions pending for about to be committed transaction %s: %s' % (tx_id, self.tx_waiting[tx_id]))
                del self.tx_waiting[tx_id]

            tx.status = TxStatus.COMMITTED
            tx.commit_time = start_time
            # TODO: Commit all dirty variables to ensure variable lookup doesn't need to check if
            # transaction is committed of not.
            print_('{1}> {0} Committed'.format(tx_id, start_time))
            for accessed_site in tx.sites_accessed:
                self._get_site(accessed_site).commit(tx_id)

        # At commit time, release all the lock.
        for data_manager in self.sites:
            data_manager.status == SiteStatus.UP and data_manager.lock_release_phase(tx_id)

        # Remove all waits for edges for this transaction.
        for other_tx_id in TX_MAP:
            if tx_id in TX_MAP[other_tx_id].waits_for:
                TX_MAP[other_tx_id].waits_for.remove(tx_id)

        self.try_pending_instructions = True


    def dump(self, time, updated_only=False):
        # dump() prints out the committed values of all copies of all variables at
        # all sites, sorted per site with all values per site in ascending order by variable
        # name, e.g.
        # site 1 – x2: 6, x3: 2, ... x20: 3
        # ...
        # site 10 – x2: 14, .... x8: 12, x9: 7, ... x20: 3
        # This includes sites that are down (which of course should not reflect writes
        # when they are down).
        # in one line.
        # one line per site.

        self._try_pending(time)
        if updated_only:
            # Only for debugging to show modified value.
            # TODO: Change updated_only to default False before submiting.
            for site in self.sites:
                print('site {0} - {1}'.format(
                    site.site_index,
                    ', '.join(['{0}: {1}'.format('x%d' % i, site.data['x%d.%d' % (i, site.site_index)].val)
                              for i in range(1, 21)
                              if ('x%d.%d' % (i, site.site_index)) in site.data and site.data['x%d.%d' % (i, site.site_index)].val != 10 * i])))
        else:
          for site in self.sites:
              print ('site {0} - {1}'.format(
                  site.site_index,
                  ', '.join(['{0}: {1}'.format('x%d' % i, site.data['x%d.%d' % (i, site.site_index)].val)
                            for i in range(1, 21) if ('x%d.%d' % (i, site.site_index)) in site.data])))

    def quit(self, time):
        '''Last instruction.'''
        self._try_pending(time)
    
    def fail(self, site_index, time):
        '''Handler when transaction manager notices site failed.'''
        self._try_pending(time)
        self._get_site(site_index).fail()
        print_('{1}> Site {0} failed'.format(site_index, time))

    def recover(self, site_index, time):
        '''Handler when tx manager notice site is up.'''
        self._try_pending(time)
        self._get_site(site_index).recover(time)
        self.try_pending_instructions = True
        print_('{1}> Site {0} recovered'.format(site_index, time))


class Driver(object):
    '''Reads input from console and issue DB transactions.'''
    def __init__(self):
        self.time = 0
        self.tx_manager = TransactionManager(10)
    
    def execute(self, line):
        self.time = self.time + 1
        if re.fullmatch('begin\s*\((.*)\)', line):
            match = re.fullmatch('begin\s*\((.*)\)', line)
            tx_id = match.group(1).strip()
            self.tx_manager.begin_tx(tx_id, self.time)
        elif re.fullmatch('beginRO\s*\((.*)\)', line):
            match = re.fullmatch('beginRO\s*\((.*)\)', line)
            tx_id = match.group(1).strip()
            self.tx_manager.begin_tx_read_only(tx_id, self.time)
        elif re.fullmatch('R\s*\((.*),(.*)\)', line):
            match = re.fullmatch('R\s*\((.*),(.*)\)', line)
            tx_id, var_name = match.group(1).strip(), match.group(2).strip()
            self.tx_manager.read(tx_id, var_name, self.time)
        elif re.fullmatch('W\s*\((.*),(.*)\,(.*)\)', line):
            match = re.fullmatch('W\s*\((.*),(.*)\,(.*)\)', line)
            tx_id, var_name, value = match.group(1).strip(), match.group(2).strip(), int(match.group(3).strip())
            self.tx_manager.write(tx_id, var_name, value, self.time)
        elif re.fullmatch('end\s*\((.*)\)', line):
            match = re.fullmatch('end\s*\((.*)\)', line)
            tx_id = match.group(1).strip()
            self.tx_manager.try_commit(tx_id, self.time)
        elif re.fullmatch('dump\s*\(\)', line):
            self.tx_manager.dump(self.time)
        elif re.fullmatch('fail\s*\((.*)\)', line):
            match = re.fullmatch('fail\s*\((.*)\)', line)
            site_index = int(match.group(1).strip())
            self.tx_manager.fail(site_index, self.time)
        elif re.fullmatch('recover\s*\((.*)\)', line):
            match = re.fullmatch('recover\s*\((.*)\)', line)
            site_index = int(match.group(1).strip())
            self.tx_manager.recover(site_index, self.time)
            
    def run(self, input):
        INPUT_STREAM = input.split('\n')
        for line in INPUT_STREAM:
            if line.strip().startswith('#') or line.strip().startswith('//') or not line:
                continue
            if line == 'quit':
                break
            self.execute(line.strip())
        self.tx_manager.quit(self.time)

'''
If the TM requests a read for transaction T and cannot get it due to
failure, the TM should try another site (all in the same step). If no relevant
site is available, then T must wait. This applies to read-only transactions
as well which, for each variable x, must have access to the version of x that
was the last to commit before the transaction begins. T may also have to
wait for conflicting locks. Thus the TM may accumulate an input command
for T and will try it on the next tick (time moment). As mentioned above,
while T is blocked (whether waiting for a lock to be released or a failure to
be cleared), our test files will emit no new operations for T.
If a site (also known as a data manager or DM) fails and recovers, the
DM would normally decide which in-flight transactions to commit (perhaps
by asking the TM about transactions that the DM holds pre-committed but
not yet committed), but this is unnecessary since, in the simulation model,
commits are atomic with respect to failures (i.e., all writes of a committing
transaction apply or none do). Upon recovery of a site s, all non-replicated
variables are available for reads and writes. Regarding replicated variables,
the site makes them available for writing, but not reading. In fact, a read
for a replicated variable x will not be allowed at a recovered site until a
committed write to x takes place on that site (see lecture notes on recovery
when using the available copies algorithm).



During execution, your program should say which transactions commit
and which abort and why. For debugging purposes you should implement
(for your own sake) a command like querystate() which will give the state
of each DM and the TM as well as the data distribution and data values.
Finally, each read that occurs should show the value read.
If a transaction T accesses an item (really accesses it, not just request
a lock) at a site and the site then fails, then T should continue to execute
and then abort only at its commit time (unless T is aborted earlier due to
deadlock).
9
4.1.1 Running the programming project
You will demonstrate the project to our very able graders. You will have
15-30 minutes to do so. The test should take a few minutes. The only
times tests take longer are when the software is insufficiently portable. The
version you send in should run on departmental servers or on your laptop.
The grader will ask you for a design document that explains the structure
of the code (see more below). The grader will also take a copy of your source
code for evaluation of its structure (and, alas, to check for plagiarism).
4.1.2 Documentation and Code Structure
Because this class is meant to fulfill the large-scale programming project
course requirement, please give some thought to design and structure. The
design document submitted in late October should include all the major
functions, their inputs, outputs and side effects. If you are working in a
team, the author of the function should also be listed. That is, we should
see the major components, relationships between the components, and communication between components. This should work recursively within components. A figure showing interconnections and no more than three pages
of text would be helpful.
The submitted code similarly should include as a header: the author (if
you are working in a team of two), the date, the general description of the
function, the inputs, the outputs, and the side effects.
In addition, you should provide a few testing scripts in plain text to the
grader in the format above and say what you believe will happen in each
test.
Finally, you will use reprozip and virtual machines to make your project
reproducible even across different architectures for all time. Simply using
portable Java or python or whatever is not the same. Packaging using
reprozip and doing so correctly constitutes roughly 20% of the project grade.


Available Copies Algorithm
Replicated data can enhance fault tolerance
by allowing a copy of an item to be read even
when another copy is down.
Basic scheme:
Read from one copy;
Write to all copies
On read, if a client cannot read a copy of x
from a server (e.g. if client times out ), then
read x from another server.
On write, if a client cannot write a copy of x
(e.g. again, timeout), then write to all other
copies, provided there is at least one.

Available Copies Continued
• At Commit time: For two phase locked transactions, ensure that all servers that
you accessed (read or write) have been up since the first time they were accessed.
(Note: If the access was a write, this is obvious, since the write might be lost and it
would a lot of overhead to check for replicates and whether they have been written
to.) Otherwise, abort. (Note: Read-only transactions need not abort in this case.)
• When a new site is introduced or a site recovers: The unreplicated data is available immediately for reading.
Copies of replicated data items should not respond
to a read on x until a committed copy of x has been written to them.
Site recovery
0. Commit transactions that should be committed and abort the others.
1. All non-replicated items are available to
read and write.
2. Replicated parts:
Option 1: Quiesce the system (allow all transactions to end, but don’t start new ones) and
have this site read copies of its items from
any other up site. When done, this service
has recovered.
Option 2: The site is up immediately. Allow
writes to copies. Reject reads to x until a
write to x has occurred.
Reason for Abort on Failure
Suppose we don’t abort when sites that we
have read from fail. Assume that lock managers are local to sites.
Suppose we have sites A, B, C, and D. A and
B have copies of x (xA and xB) and C and D
have copies of y (yC and yD).
T1: R(x) W(y)
T2: R(y) W(x)
Suppose we have R1(xA) R2(yD) site A fails;
site D fails; W1(yC) W2(xB)
Here we have an apparently serializable computation, yet transaction 2 reads y before transaction 1 writes it and transaction 1 reads x
before transaction 2 writes it.
Available Copies Problems
Suppose T1 believes site A is down but T2
reads from it. This could easily give us a nonserializable execution, since T2 might not see
T1’s write to a variable that A holds.
So, when T1 believes a site is down, all sites
must agree. This implies no network partitions.
If network partitions are possible, then use a
quorum. In simplest form: all reads and all
writes must go to a majority of all sites.
'''

# Global Declarations START HERE

VARS = []
VARS_MAP = {}

# Map from transaction id to transaction object.
# TODO: Transaction object need to be created and stored here as they come.
TX_MAP = {}

DUMP = '\ndump()'
def initialize_global_store():
    global VARS
    global VARS_MAP
    global TX_MAP
    global INPUT
    VARS = [ [DataVariable(index, 10 * index, replica if index % 2 == 0 else 1 + (index % 10))
             for replica in range(1, 11 if index % 2 == 0 else 2)] for index in range(1, 21)]
    VARS_MAP = {v.fullname: v for rv in VARS for v in rv}
    TX_MAP = {}
    INPUT = ''

def run_and_save_output(input_filename, output_filename, dump_at_end):
    with open(input_filename, 'r') as ip_file:
        if not output_filename:
            output_filename = 'Output_File_For_' + input_filename
        with open(output_filename, 'w') as op_file:
            input_txt = ip_file.read()
            temp_stdout = sys.stdout
            sys.stdout = op_file
            if dump_at_end and not input_txt.endswith(DUMP):
                input_txt += DUMP
            Driver().run(input_txt)
            sys.stdout = temp_stdout
    initialize_global_store()

''' Used for verbose mode '''
verbose = True

INPUT = '''
// Test 3
// T1 should not abort because its site did not fail.
// In fact all transactions commit
// x8 has the value 88 at every site except site 2 where it won't have
// the correct value right away but must wait for a write to take place.
begin(T1)
begin(T2)
R(T1,x3)
fail(2)
W(T2,x8,88) 
R(T2,x3)
W(T1, x5,91)
end(T2)
recover(2)
end(T1)
'''

# Global Declarations END HERE

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  # python main.py --input sample_tests
  parser.add_argument('--input', help = 'Input file name')
  parser.add_argument('--output', help = 'Output file name', default = '')
  parser.add_argument('--batch', help = 'Is batch mode? True/False', default = False)
  parser.add_argument('--v', help = 'Verbose Mode (print only dump or all statements): True/False', default = True)
  parser.add_argument('--dumpAtEnd', help = 'Always dump() on completion of test case: True/False', default = True)
  args = parser.parse_args()
  verbose = args.v
  initialize_global_store()
  if not args.batch:
      run_and_save_output(args.input, args.output, args.dumpAtEnd)
  else:
    # Batch mode - input file has names of multiple input files and output file has names of corresponding output files
    batch_input = open(args.input, 'r').read().split()
    batch_output = ['Output_File_For_' + ip for ip in batch_input]
    if args.output:
        with open(args.output, 'r') as op_file:
            op_content = op_file.read().split('\n')
            if op_content:
                batch_output = op_content

    for input_filename, output_filename in zip(batch_input, batch_output):
        run_and_save_output(input_filename, output_filename,args.dumpAtEnd)
        
    
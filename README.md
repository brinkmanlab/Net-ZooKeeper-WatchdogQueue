Net-ZooKeeper-WatchdogQueue
===========================

Watchdog and Queue wrapper around ZooKeeper using Net::ZooKeeper

PURPOSE
=======

This was designed to meet an internal need, where processes in the Torque
resource management system needed to be coordinated, there's no means to
have a process execute after a series of other processes completes (even
using epilogue scripts in an array job causes the scriptto run after *each*
piece finishes, not when all pieces finish).  So ZooKeeper is being used to
monitor a series of processes, and when all these processes detach from
ZooKeeper, and don't hang too long, the higher level process will know
its safe to proceed to the next step.  The queue portion of the module
can probably be ignored if it doesn't fit your needs and used only
as a watchdog module.

SYNOPSIS
========

    use Net::ZooKeeper::WatchdogQueue;

    $watchdog = new Net::ZooKeeper::WatchdogQueue('host1:7000,host2:7000',
                                                  '/mywatchdog');
    $watchdog = new Net::ZooKeeper::WatchdogQueue('host1:7000,host2:7000',
                                                  '/mywatchdog',
                                                  'session_timeout' => $session_timeout,
                                                  'session_id' => $session_id);

    # From master
    $watchdog->create_queue(timer => 120,       # Max seconds until a timer
                                                # is considered expired
                               queue => \@items,
                                                # An array of items to put in the queue
                               sync_start => 1);# Create a barrier the first
                                                # client should remove
    # Launch children
    $synced = $watchdog->wait_sync(120);        # Wait up to 120 seconds for
                                                # a child to launch
    $count = $watchdog->queue_count();          # We can also wait to ensure the queue
                                                # is empty before checking for failed jobs
    ($alive, $expired) = $watchdog->check_timers();
                                                # Find how many times are left
                                                # and how many have expired
    $timers = $watchdog->get_timers(1)          # Fetch all timers and their
                                                # alive time, optional parameter
                                                # to only return expired timers

    $watchdog->clear_timers();                  # Remove the time, clean up

    # From client
    $watchdog->create_timer('process_identifier');
                                                # Create a timer
    $task = $watchdog->consume();               # Fetch an item from the queue
                                                # return either the item or undef if
                                                # we're unsuccessful

    # Do work....
    $watchdog->kick_dog();                      # Touch the timer occasionally
                                                # to reset it


DESCRIPTION
===========

For distributed applications, the task master opens
and creates a a root node in the ZooKeeper tree before
spawning children.  It also fills a queue with tasks to complete.

This package has a dual purpose, the master can ensure all
children have started and watch for when they all complete or
if any timeout while running.

Children upon creation make ephemeral child nodes, and should
periodically "touch" the node to show they're still alive. They
also need to consume a queue item.

Master watches the tree for all child nodes to either vanish or
for a pre-determined time to pass with a child node not being
updated.

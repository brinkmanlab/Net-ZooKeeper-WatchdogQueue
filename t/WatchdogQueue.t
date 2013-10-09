# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test::More;
BEGIN { plan tests => 7 };

use Net::ZooKeeper::WatchdogQueue;

$watchdog = new Net::ZooKeeper::WatchdogQueue('localhost:2181',
                                                  '/mywatchdog');
@tasks = qw{one two};

$watchdog->create_queue(time => 120, queue => \@tasks,
			sync_start => 1);

$count = $watchdog->queue_count();
ok($count == 2, "Two items in the queue");

$watchdog->consume();
$count = $watchdog->queue_count();
ok($count == 1, "One item in the queue now");

$ret = $watchdog->wait_sync(2);
ok($ret == 0, "Sync timed out");

$watchdog->create_timer('process_identifier');
sleep 1;
($alive, $expired) = $watchdog->check_timers();
ok($alive == 1, "One timer is alive");
ok($expired == 0, "No timers expired");

$ret = $watchdog->kick_dog();
ok($ret == 1, "Timer successfully reset");

$ret = $watchdog->wait_sync(2);
ok($ret == 1, "Sync is clear");

$watchdog->clear_timers();

#sleep 120;
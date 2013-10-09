# Net::ZooKeeper::WatchdogQueue

use 5.008_008;

use Config;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'          => 'Net::ZooKeeper::WatchdogQueue',
    'VERSION_FROM'  => 'lib/Net/ZooKeeper/WatchdogQueue.pm',
    'PREREQ_PM'     => {'Net::ZooKeeper' => 0 },
    'ABSTRACT_FROM' => 'lib/Net/ZooKeeper/WatchdogQueue.pm',
    'AUTHOR'        => 'Matthew Laird <lairdm@sfu.ca>',
);

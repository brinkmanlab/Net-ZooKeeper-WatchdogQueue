=head1 NAME

Net::ZooKeeper::WatchdogQueue - Perl process watchdog
using ZooKeeper as the backend

=head1 SYNOPSIS

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


=head1 DESCRIPTION

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

=head1 MAINTAINER

  Matthew Laird <lairdm@sfu.ca>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

=cut

package Net::ZooKeeper::WatchdogQueue;

use strict;
use Net::ZooKeeper qw(:node_flags :acls);

use vars qw($VERSION);$VERSION = 0.1;

sub new {
    my ($class, $hosts, $root, @args) = @_;
    my $self = { root => $root };

    $self->{zkh} = Net::ZooKeeper->new($hosts, @args);

    bless $self, $class;
    return $self;    
}

# Create a watchdog/queue, set the alarm timer to 120 seconds
# unless otherwise specified

sub create_queue {
    my $self = shift;
    my (%args) = @_;
    $self->{timer} = do { $args{timer} ? $args{timer} : 120 };

    die "Error, root $self->{root} already exists"
	if($self->{zkh}->exists($self->{root}));

    $self->{zkh}->create($self->{root}, 'watchdog',
	                 'acl' => ZOO_OPEN_ACL_UNSAFE) or
	die "Unable to create root $self->{root}";

    # Put the objects in the queue
    foreach my $task (@{$args{queue}}) {
	$self->{zkh}->create($self->{root} . '/queue-', $task,
			     'flags' => (ZOO_SEQUENCE),
			     'acl' => ZOO_OPEN_ACL_UNSAFE
	                    );
    }

    if($args{sync_start}) {
	 $self->{zkh}->create($self->{root} . '/sync', 'sync',
			      'acl' => ZOO_OPEN_ACL_UNSAFE) or
	     die "Error, unable to create sync node";
    }
}

# This really isn't needed, we get all the information we need
# to attach to an existing queue through the constructor,
# but this allows a nicer OO model to ensure the queue
# does indeed exist

sub attach_queue {
    my $self = shift;
    my (%args) = @_;
    $self->{timer} = do { $args{timer} ? $args{timer} : 120 };

    die "Error, root $self->{root} doesn't exist"
	unless($self->{zkh}->exists($self->{root}));
    
}

sub create_timer {
    my $self = shift;
    my $process_id = shift;
    my $sequential = shift;

    # We'll make the path, sequential just for convenience if asked (it
    # really doesn't matter I guess, just prevents NS collisions)
    # and emphemeral so it vanishes if the process dies.
    my $path = $self->{zkh}->create($self->{root} . '/timer-', $process_id,
				    'flags' => (ZOO_EPHEMERAL | ($sequential ? ZOO_SEQUENCE : 0)),
				    'acl' => ZOO_OPEN_ACL_UNSAFE
	                            );

    # Error, can't make the path
    unless($path) {
	die "Error, can not create timer: " . $self->{zkh}->get_error();
    }

    # Save our path for later
    $path =~ /timer-(\d+)$/;
    $self->{timer_id} = $1;
    $self->{path} = $path;
    $self->{process_id} = $process_id;

    # Try to remove the sync child node if it exists
    $self->{zkh}->delete($self->{root} . '/sync')
	if($self->{zkh}->exists($self->{root} . '/sync'));
}

# Wait for the sync node to be removed or a timeout
# to be reached
# Returns 1 if a child has started
# otherwise return 0 if the timer expired

sub wait_sync {
    my $self = shift;
    my $timeout = do { @_ ? shift : 120 };

    my $watch = $self->{zkh}->watch('timeout' => (120*10));
    # Attach the watch, if the node already doesn't exist
    # that's good, we're done!
    unless($self->{zkh}->exists($self->{root} . '/sync', 
				'watch' => $watch)) {
	return 1;
    }

    if($watch->wait()) {
	# Hard code if the node was deleted because the module
	# code is odd
	if($watch->{event} == 2) {
	    return 1;
	}
    }

    # The watch timed out, we're still not synced
    return 0;
}

# Tries to update the path so the mtime changes,
# returns true if successful, otherwise false
# Possible false situation is the master has exited
# and cleared the timers when it went

sub kick_dog {
    my $self = shift;
    
    return $self->{zkh}->set($self->{path}, $self->{process_id});
}

# Pull an item out of the queue and return it if we can delete it,
# otherwise return undef if we can't successfully return an item

sub consume {
    my $self = shift;

    foreach my $path ($self->{zkh}->get_children($self->{root})) {
	# Ensure its a queue child
	next unless($path =~ /queue-/);

	# Get the item in the queue, we really don't care about order
	# in this context
	my $task = $self->{zkh}->get($self->{root} . '/' . $path);

	# Return it only if we can successfully delete it,
	# otherwise try again with the next item
	if($self->{zkh}->delete($self->{root} . '/' . $path)) {
	    return $task;
	}
    }

    return undef;
}

# Get the number of items in the queue

sub queue_count {
    my $self = shift;

    my $count = 0;

    foreach my $path ($self->{zkh}->get_children($self->{root})) {
	# Ensure its a queue child
	next unless($path =~ /queue-/);

	$count++;
    }

    return $count;
}

# Check the watched processes, return how many are
# still around and how many have expired timers

sub check_timers {
    my $self = shift;

    my $alive = 0; my $expired = 0;
    foreach my $path ($self->{zkh}->get_children($self->{root})) {
	# Ensure its a timer child
	next unless($path =~ /timer-/);

	# How long has it been since the node was last touched?
	my $t_alive = $self->time_alive($path);

	# Just a sanity check, ensure the node was still
	# alive when we went to check it (race condition)
	if($t_alive >= 0) {
	    $alive++;
	    # If the timer is longer than what we consider
	    # alright, it's expired
	    $expired++ if($t_alive > $self->{timer});		
	}
    }

    return ($alive, $expired);
}

# Fetch all the process_ids and their time since
# last being touched.  Optional parameter for expired
# timers only

sub get_timers {
    my $self = shift;
    my $expired_only = do { @_ ? shift : 0 };

    my $timers;

    foreach my $path ($self->{zkh}->get_children($self->{root})) {
	# Ensure its a timer child
	next unless($path =~ /timer-/);

	my $t_alive = $self->time_alive($path);

	next if(($t_alive <= $self->{timer}) && $expired_only);

	my $process_id = $self->{zkh}->get($self->{root} . '/' . $path);

	# Save the pair of the timer's process_id and the 
	# time alive
	$timers->{$process_id} = $t_alive;
    }

    return $timers;

}

sub clear_timers {
    my $self = shift;

    # Clear all the children's timers, not nice, but it has
    # to be done, they'll understand...
    foreach my $path ($self->{zkh}->get_children($self->{root})) {
	$self->{zkh}->delete($self->{root} . '/' . $path);
    }

    # And clean up our main node
    $self->{zkh}->delete($self->{root});
}

sub time_alive {
    my $self = shift;
    my $node = shift;

    my $stat = $self->{zkh}->stat();
    if($self->{zkh}->exists($self->{root} . '/' . $node,
			    'stat' => $stat)){
	# If we received the node, return the time
	# in seconds it's been alive
	return (time - int($stat->{mtime}/1000));
    } else {
	# Did we hit a race condition? Is the node gone now?
	return -1;
    }
}

1;


# System Configuration

Several system configuration settings should be set before running BanyanDB. 

## Remove System Limits

BanyanDB requires a large number of open files and a large number of processes. The default system limits are not sufficient for BanyanDB.

```sh
ulimit -l unlimited
ulimit -n 800000
ulimit -u 8096
```

`ulimit -l unlimited`: This command sets the maximum size of memory that can be locked into RAM to unlimited. Locking memory can be important for performance-critical applications that need to ensure certain data remains in physical memory and is not swapped out to disk.

`ulimit -n 800000`: This command sets the maximum number of open file descriptors to 800,000. File descriptors are references to open files, sockets, or other input/output resources. Increasing this limit is often necessary for applications that handle a large number of simultaneous connections or open files.

`ulimit -u 8096`: This command sets the maximum number of user processes to 8,096. This limit controls how many processes a single user can create. Raising this limit can be important for applications that spawn many subprocesses or threads.

## NTP Setup

BanyanDB requires accurate time synchronization across all servers. We recommend setting up NTP (Network Time Protocol) to ensure that all servers have synchronized clocks.

But the nodes in a cluster are independently obtaining their time from random pool servers out on the Internet, the chances that two nodes can have widely (by NTP standards) differing time is high. This can cause problems in a distributed system like BanyanDB. In order to avoid this, we recommend setting up a local NTP server that all nodes in the cluster can sync to.

## Kernel Configuration

BanyanDB requires a few kernel parameters to be set to ensure optimal performance. These parameters can be set by adding the following lines to `/etc/sysctl.conf`:

```sh

# Make the system very reluctant to swap memory to disk
vm.swappiness = 1
```

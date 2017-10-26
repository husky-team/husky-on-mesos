# husky-on-mesos

# Cpp

To be added...


# Python

1. Install Mesos and start Mesos(Mesos master and agents) on cluster.
2. Build Husky(Husky Master and Application) and copy the builts to a path where all machines can access.
3. Run commands as the following(Note: __`4`__ means __cpus__, __`32`__ means __mems__):

        $ cd python
        $ python husky_framework.py Mesos-master-ip:master-port 4 32 /path/to/husky/built

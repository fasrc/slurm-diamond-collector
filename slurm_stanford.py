#!/usr/bin/env python
# vim: tabstop=4 softtabstop=4 expandtab ft=python

#==============================================================================
# Sherlock Cluster
#
# File: slurm_stanford.py
# Description: Diamond collector to gather Slurm metrics for monitoring
#
# Authors  : Stephane Thiell <sthiell@stanford.edu>
#            Kilian Cavalotti <kilian@stanford.edu>
#            Paul Edmon <pedmon@cfa.harvard.edu>
# Created  : 2015/06/19
# Updated  : 2015/06/24 - slurm features and gres
#            2015/06/26 - improved nodes/cpu states
#            2015/07/06 - sshare
#            2017/04/18 - Sherlock 2.0 support
#            2019/10/24 - Adapted to diamond for general use
#
#==============================================================================

"""
Slurm metrics

Gather Slurm metrics and output them in Graphite/Carbon format

This script is designed to minimize load on the Slurm controller by reducing
the number of calls to squeue, sdiag, etc.
"""

import re, sys, argparse
import diamond.collector

from datetime import datetime
from string import maketrans
from time import mktime, time

## -- Helpers -----------------------------------------------------------------

def strdate_to_ts(s):
    """
    Helper function to convert human readable sdiag date into timestamp
    """
    return int(mktime(
        datetime.strptime(s.strip(), "%a %b %d %H:%M:%S %Y").timetuple()))

## -- main program ------------------------------------------------------------

class SlurmStanfordCollector(diamond.collector.Collector):
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SlurmStanfordCollector, self).get_default_config()
        config.update({
            'path': 'stanford'
        })
        return config

    def collect(self):

        ### SQUEUE ####
        """event handler initializer: declare dicts"""
        # (group, user, partition, gres, state) => jobs (int)
        self.jobs = {}
        # (group, user, partition, gres, state) => cpus (int)
        self.cpus = {}
        # (group, user, partition, gres, state, reason) => jobs (int)
        self.jobs_r = {}
        # (group, user, partition, gres, state, reason) => cpus (int)
        self.cpus_r = {}
        # (group, user, partition, gres, state) => gpus (int)
        self.gpus = {}

        try:
            # Grab data from slurm
            proc = subprocess.Popen(
                    "/usr/bin/squeue -rh -o '%g %u %P %16b %T %C %D %R'"
                    ),stdout=subprocess.PIPE
            )
        except Exception:
            self.log.exception("error occured fetching job hash")
            return
        else:
			for line in proc.stdout:
                """read line from squeue command"""
                try:
                    # workaround for Slurm 18
                    line = re.sub(' +', ' ', line)
                    #
                    group, user, partition, gres, state, cpus, nodes, reason = \
                        line.split(' ', 7)
                    # workaround for Slurm 18
                    if gres == 'N/A':
                        gres = 'null'
                except ValueError:
                    print >>sys.stderr, "LINE PARSED: %s" % line
                    raise
                # without reason
                key = (group, user, partition, gres, state)
                try:
                    self.jobs[key] += 1
                    self.cpus[key] += int(cpus)
                except KeyError:
                    self.jobs[key] = 1
                    self.cpus[key] = int(cpus)

                # with reason
                reason = reason.split(',')[0]
                if reason.startswith('('):
                    reason = reason.strip('(').strip(')').replace(' ', '_')
                    key = (group, user, partition, gres, state, reason)
                    try:
                        self.jobs_r[key] += 1
                        self.cpus_r[key] += int(cpus)
                    except KeyError:
                        self.jobs_r[key] = 1
                        self.cpus_r[key] = int(cpus)

                # GPUs
                try:
                    key = (group, user, partition, gres, state)
                    # gres returns the number of gpus per nodes
                    if (gres.count(":") == 1):
                        gres = gres.split(':')
                    else:
                        gres = 'gpu:1'
                        gres = gres.split(':')

                    if gres[0] == 'gpu':  # gres type
                        gpus = int(gres[-1]) * int(nodes)
                        try:
                            self.gpus[key] += gpus
                        except KeyError:
                            self.gpus[key] = gpus
                except IndexError:  # ignore error if we can't parse
                    pass

        """squeue command done: print results extracted from dicts"""
        # without reason
        for (group, user, partition, gres, state), jobs in self.jobs.items():
            out="squeue.%s.%s.%s.%s.%s.jobs" % (
                group, user, partition, re.sub('[()]','', gres), state)
            self.publish(out, jobs)
        for (group, user, partition, gres, state), cpus in self.cpus.items():
            out="squeue.%s.%s.%s.%s.%s.cpus %d" % (
                group, user, partition, re.sub('[()]','', gres), state)
            self.publish(out, cpus)
        # with reason
        for (group, user, partition, gres, state, reason), jobs in self.jobs_r.items():
            out="squeue.%s.%s.%s.%s.%s.reasons.%s.jobs" % (
                group, user, partition, re.sub('[()]','', gres), state, reason)
            self.publish(out, jobs)
        for (group, user, partition, gres, state, reason), cpus in self.cpus_r.items():
            out="squeue.%s.%s.%s.%s.%s.reasons.%s.cpus" % (
                group, user, partition, re.sub('[()]','', gres), state, reason)
            self.publish(out, cpus)
        # GPUs
        for (group, user, partition, gres, state), gpus in self.gpus.items():
            out="squeue.%s.%s.%s.%s.%s.gpus" % (
                group, user, partition, re.sub('[()]','', gres), state)
            self.publish(out, gpus)


        ###SINFO###

        """initalizer: compile regexp pattern used to parse sinfo output"""
        self.pattern = re.compile(
            r"(?P<partition>.*)\s(?P<mem>\d*)\s(?P<cpu>\d*)\s"
            r"(?P<features>.*)\s(?P<gres>.*)\s"
            r"(?P<state>[^*$~#]*)[*$~#]?\s(?P<nodecnt>\d*)\s"
            r"(?P<allocated>\d*)/(?P<idle>\d*)/(?P<other>\d*)/(?P<total>\d*)")
        self.transtable = maketrans('.', '_')
        self.partitions = set()
        self.nodes = {}
        self.nodes_total = {}
        self.cpus = {}
        self.cpus_total = {}

        try:
            # Grab data from slurm
            proc = subprocess.Popen(
                    "sinfo -h -e -o '%R %m %c %f %G %T %D %C'"
                    ),stdout=subprocess.PIPE
            )
        except Exception:
            self.log.exception("error occured fetching job hash")
            return
        else:
			for line in proc.stdout:
                """read line from sinfo command"""
                # owners 64000 16 CPU_IVY,E5-2650v2,2.60GHz,GPU_KPL,TITAN_BLACK,titanblack gpu:gtx:4 mixed 2 8/24/0/32
                msg = line
                match = self.pattern.match(msg)
                if match:
                    # get partition name (cleaned) and add to a set for partition_count
                    partition = match.group("partition").translate(None, '*')
                    self.partitions.add(partition)
                    features = match.group("features").translate(self.transtable, '*')
                    gres = match.group("gres")
                    # build path
                    base_path = "sinfo.%s.%s.%s.%s.%s" % ( partition,
                        match.group("mem"), match.group("cpu"), features,
                        re.sub('[()]','', gres) )

                    # build dicts to handle any duplicates and also total...

                    # nodes
                    state = match.group("state")
                    nodecnt = int(match.group("nodecnt"))

                    if base_path not in self.nodes:
                        self.nodes[base_path] = {'allocated': 0, 'completing': 0,
                                                 'down': 0, 'drained': 0,
                                                 'draining': 0, 'idle': 0,
                                                 'maint':0, 'mixed': 0, 'unknown': 0 }
                        self.nodes[base_path][state] = 0 # in case of another state
                        self.nodes_total[base_path] = 0

                    self.nodes_total[base_path] += nodecnt

                    try:
                        self.nodes[base_path][state] += nodecnt
                    except KeyError:
                        self.nodes[base_path][state] = nodecnt

                    # CPUs
                    if base_path not in self.cpus:
                        self.cpus[base_path] = { 'allocated': 0,
                                                 'idle': 0,
                                                 'other': 0 }
                        self.cpus_total[base_path] = 0

                    for cpustate in ('allocated', 'idle', 'other'):
                        self.cpus[base_path][cpustate] += int(match.group(cpustate))

                    self.cpus_total[base_path] += int(match.group('total'))

        # Print partition count
        base_path = "sinfo.partition_count"
        self.publish(base_path, len(self.partitions))
        # Print all details
        for base_path, stated in self.nodes.iteritems():
            for state, nodecnt in stated.iteritems():
                out = "%s.nodes.%s" % (base_path, state)
                self.publish(out, nodecnt)
        for base_path, totalcnt in self.nodes_total.iteritems():
            out = "%s.nodes_total" % (base_path)
            self.publish(out, totalcnt)
        for base_path, stated in self.cpus.iteritems():
            for state, cpucnt in stated.iteritems():
                out = "%s.cpus.%s" % (base_path, state)
                self.publish(out, cpucnt)
        for base_path, totalcnt in self.cpus_total.iteritems():
            out = "%s.cpus_total" % (base_path)
            self.publish(out, totalcnt)

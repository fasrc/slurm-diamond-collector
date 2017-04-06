#!/usr/bin/python
"""
slurm_job_leaderboard.py

Who has the most jobs on the cluster?
"""
import re
import subprocess
from datetime import datetime

import diamond.collector

METRIC_CHARS_RE = re.compile("[^a-zA-Z0-9_-]")
METRIC_NAME_MAX_LEN = 255


class SlurmJobLeaderboardCollector(diamond.collector.Collector):

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SlurmJobLeaderboardCollector, self).get_default_config()
        config.update({
            'path': 'leaderboard'
        })
        return config

    def get_job_stats(self):
        proc = subprocess.Popen(
            'sacct -S {hour}:00 -n -P -o user,state'.format(
                hour=datetime.now().hour
            ).split(),
            stdout=subprocess.PIPE
        )
        data = [l.strip().split('|') for l in proc.stdout.readlines() if
                l.split('|')[0]]
        totals = {}
        stats = dict(total=totals)
        for user, job_state in data:
            job_state = job_state.lower().split()[0]
            job_state_stats = stats.get(job_state, {})
            if user in job_state_stats:
                job_state_stats[user] += 1
            else:
                job_state_stats[user] = 1
            if user in totals:
                totals[user] += 1
            else:
                totals[user] = 1
            stats[job_state] = job_state_stats
        return stats

    def convert2metric(self, string):
        return METRIC_CHARS_RE.sub('_', string)[:METRIC_NAME_MAX_LEN - 1]

    def collect(self):
        """
        Collect job counts per user
        """
        try:
            job_stats = self.get_job_stats()
        except Exception:
            self.log.exception("error occured fetching job hash")
            return
        for state, user_counts in job_stats.iteritems():
            for user, count in user_counts.iteritems():
                metric_name = '{state}.{user}'.format(
                    state=self.convert2metric(state),
                    user=self.convert2metric(user)
                )
                self.publish(metric_name, count)

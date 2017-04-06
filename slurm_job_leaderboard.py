#!/usr/bin/python
"""
slurm_job_leaderboard.py

Who has the most jobs on the cluster?
"""

import subprocess
from datetime import datetime

import diamond.collector


class SlurmJobsLeaderboard(diamond.collector.Collector):

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SlurmJobsLeaderboard, self).get_default_config()
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
        stats = {}
        for user, job_state in data:
            job_state = job_state.lower()
            job_state_stats = stats.get(job_state, {})
            if user in job_state_stats:
                job_state_stats[user] += 1
            else:
                job_state_stats[user] = 1
            stats[job_state] = job_state_stats
        return stats

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
                metric_name = '{state}.{user}'.format(state=state, user=user)
                self.publish(metric_name, count)

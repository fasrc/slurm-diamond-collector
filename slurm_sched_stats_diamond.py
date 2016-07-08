#!/usr/bin/python

"""
slurm_sched_stats_diamond.py
A script that uses PySlurm to get the slurm scheduler statistics.
"""

import sys,os,json
import pyslurm
import diamond.collector

class SlurmSchedStatsCollector(diamond.collector.Collector):
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SlurmSchedStatsCollector, self).get_default_config()
        config.update({
            'path':     'sdiag'
        })
        return config

    def collect(self):

        try:
            sdiag = pyslurm.statistics().get()
        except:
            return
        else:

            # Slurmctld Stats
            self.publish('server_thread_count',sdiag.get("server_thread_count"))
            self.publish('agent_queue_size',sdiag.get("agent_queue_size"))
    
            # Jobs Stats
            self.publish('jobs_submitted',sdiag.get("jobs_submitted"))
            self.publish('jobs_started',sdiag.get("jobs_started"))
            self.publish('jobs_completed',sdiag.get("jobs_completed"))
            self.publish('jobs_canceled',sdiag.get("jobs_canceled"))
            self.publish('jobs_failed',sdiag.get("jobs_failed"))
    
            # Main Scheduler Stats
            self.publish('main_last_cycle',sdiag.get("schedule_cycle_last"))
            self.publish('main_max_cycle',sdiag.get("schedule_cycle_max"))
            self.publish('main_total_cycles',sdiag.get("schedule_cycle_counter"))
    
            if sdiag.get("schedule_cycle_counter") > 0:
                self.publish('main_mean_cycle',
                    sdiag.get("schedule_cycle_sum") / sdiag.get("schedule_cycle_counter")
                )
                self.publish('main_mean_depth_cycle', (
                    sdiag.get("schedule_cycle_depth") / sdiag.get("schedule_cycle_counter")
                ))
    
            if (sdiag.get("req_time") - sdiag.get("req_time_start")) > 60:
                self.publish('main_cycles_per_minute', (
                    sdiag.get("schedule_cycle_counter") /
                    ((sdiag.get("req_time") - sdiag.get("req_time_start")) / 60)
                ))
    
            self.publish('main_last_queue_length',sdiag.get("schedule_queue_len"))
    
            # Backfilling stats
            self.publish('bf_total_jobs_since_slurm_start',sdiag.get("bf_backfilled_jobs"))
            self.publish('bf_total_jobs_since_cycle_start',sdiag.get("bf_last_backfilled_jobs"))
            self.publish('bf_total_cycles',sdiag.get("bf_cycle_counter"))
            self.publish('bf_last_cycle',sdiag.get("bf_cycle_last"))
            self.publish('bf_max_cycle',sdiag.get("bf_cycle_max"))
            self.publish('bf_queue_length',sdiag.get("bf_queue_len"))
    
            if sdiag.get("bf_cycle_counter") > 0:
                self.publish('bf_mean_cycle', (
                    sdiag.get("bf_cycle_sum") / sdiag.get("bf_cycle_counter")
                ))
                self.publish('bf_depth_mean', (
                    sdiag.get("bf_depth_sum") / sdiag.get("bf_cycle_counter")
                ))
                self.publish('bf_depth_mean_try', (
                    sdiag.get("bf_depth_try_sum") / sdiag.get("bf_cycle_counter")
                ))
                self.publish('bf_queue_length_mean', (
                    sdiag.get("bf_queue_len_sum") / sdiag.get("bf_cycle_counter")
                ))
    
            self.publish('bf_last_depth_cycle',sdiag.get("bf_last_depth"))
            self.publish('bf_last_depth_cycle_try',sdiag.get("bf_last_depth_try"))

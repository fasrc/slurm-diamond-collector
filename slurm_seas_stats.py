#!/usr/bin/python3

"""
slurm_seas_stats.py
A script to get stats for SEAS.
"""

import sys,os,json,subprocess,shlex
import diamond.collector

class SlurmSeasStatsCollector(diamond.collector.Collector):
  def get_default_config(self):
    """
    Returns the default collector settings
    """
    config = super(SlurmSeasStatsCollector, self).get_default_config()
    config.update({
    'path':     'seas'
    })
    return config

  def collect(self):

    try:
      proc = subprocess.Popen(['/usr/bin/squeue',
      '--account=acc_lab,aizenberg_lab,amin_lab,anderson_lab,aziz_lab,barak_lab,bertoldi_lab,brenner_lab,capasso_lab,chen_lab_seas,chong_lab_seas,clarke_lab,doshi$
      '--Format=RestartCnt,PendingTime',
      '--noheader',
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      return
    else:
      rtot = 0
      ptot = 0
      jcnt = 0

      for line in proc.stdout:
        (RestartCnt, PendingTime) = (" ".join(line.split())).split(" ")
        rtot = int(RestartCnt) + rtot
        ptot = int(PendingTime) + ptot
        jcnt = jcnt + 1

      rave = float(rtot)/float(jcnt)
      pave = float(ptot)/float(jcnt)

      self.publish("restartave",rave)
      self.publish("pendingave",pave)

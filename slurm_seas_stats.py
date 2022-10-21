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
      '--account=acc_lab,aizenberg_lab,amin_lab,anderson_lab,aziz_lab,barak_lab,bertoldi_lab,brenner_lab,capasso_lab,chen_lab_seas,chong_lab_seas,clarke_lab,doshi-velez_lab,dwork_lab,farrell_lab,fdoyle_lab,gajos_lab,glassman_lab,hekstra_lab,hills_lab,hu_lab_seas,idreos_lab,jacob_lab,janapa_reddi_lab,jialiu_lab,jlewis_lab,kaxiras_lab,keith_lab_seas,keutsch_lab,kohler_lab,koumoutsakos_lab,kozinsky_lab,kung_lab,linz_lab,mahadevan_lab,manoharan_lab,martin_lab_seas,mazur_lab_seas,mccoll_lab,mcelroy_lab,mitragotri_lab,moorcroft_lab,nelson_lab,parkes_lab,pehlevan_lab,pfister_lab,protopapas_lab,rush_lab,seas_computing,spaepen_lab,sunderland_lab,suo_lab,tambe_lab,tziperman_lab,vadhan_lab,vlassak_lab,walsh_lab_seas,weitz_lab,wofsy_lab,wordsworth_lab,ysinger_group,yu_lab,zickler_lab',
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

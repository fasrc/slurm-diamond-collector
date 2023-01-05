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
      '--account=acc_lab,aizenberg_lab,amin_lab,anderson_lab,aziz_lab,barak_lab,bertoldi_lab,brenner_lab,capasso_lab,chen_lab_seas,chong_lab_seas,clarke_lab,doshi-velez_lab,dwork_lab,bfarrell_lab,fdoyle_lab,gajos_lab,glassman_lab,hekstra_lab,hills_lab,hu_lab_seas,idreos_lab,jacob_lab,janapa_reddi_lab,jialiu_lab,jlewis_lab,kaxiras_lab,keith_lab_seas,keutsch_lab,kohler_lab,koumoutsakos_lab,kozinsky_lab,kung_lab,linz_lab,mahadevan_lab,manoharan_lab,martin_lab_seas,mazur_lab_seas,mccoll_lab,mcelroy_lab,mitragotri_lab,moorcroft_lab,nelson_lab,parkes_lab,pehlevan_lab,pfister_lab,protopapas_lab,rush_lab,seas_computing,spaepen_lab,sunderland_lab,suo_lab,tambe_lab,tziperman_lab,vadhan_lab,vlassak_lab,walsh_lab_seas,weitz_lab,wofsy_lab,wordsworth_lab,ysinger_group,yu_lab,zickler_lab',
      '--Format=RestartCnt,PendingTime,Partition',
      '--noheader',
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      return
    else:
      rtot = 0
      ptot = 0
      jcnt = 0
      jseas = 0

      for line in proc.stdout:
        (RestartCnt, PendingTime, Partition) = (" ".join(line.split())).split(" ")

        # Summing total number of Restarts and Pending time for later average
        rtot = int(RestartCnt) + rtot
        ptot = int(PendingTime) + ptot

        # Tallying if this job is using one of these partitions
        if Partition.count('barak_gpu') > 0: jseas += 1
        if Partition.count('barak_ysinger_gpu') > 0: jseas += 1
        if Partition.count('doshi-velez') > 0: jseas += 1
        if Partition.count('huce') > 0: jseas += 1
        if Partition.count('idreos_parkes') > 0: jseas += 1
        if Partition.count('imasc') > 0: jseas += 1
        if Partition.count('jacob_dev') > 0: jseas += 1
        if Partition.count('kaxiras') > 0: jseas += 1
        if Partition.count('kaxirasgpu') > 0: jseas += 1
        if Partition.count('kozinsky') > 0: jseas += 1
        if Partition.count('mazur') > 0: jseas += 1
        if Partition.count('narang_dgx1') > 0: jseas += 1
        if Partition.count('pehlevan') > 0: jseas += 1
        if Partition.count('tambe_gpu') > 0: jseas += 1
        if Partition.count('zickler') > 0: jseas += 1
        if Partition.count('cox') > 0: jseas += 1
        if Partition.count('seas') > 0: jseas += 1

        jcnt = jcnt + 1

      # Averaging Restart Count and Pending Time
      rave = float(rtot)/float(jcnt)
      pave = float(ptot)/float(jcnt)

      self.publish("restartave",rave,precision=2)
      self.publish("pendingave",pave)
      self.publish("totseasjobs",jcnt)
      self.publish("seaspartjobs",jseas)

    try:
      proc = subprocess.Popen(['/usr/local/bin/showq',
      '-s',
      '-p',
      'seas_compute',
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      exit
    else:
      for line in proc.stdout:
        if "cores" in line:
          line = line.replace("("," ").replace(")"," ")
          summary = (" ".join(line.split())).split(" ")
          # Publishes number of used cores, total cores, used nodes, and total nodes in the seas compute partition
          self.publish("sccu",summary[4])
          self.publish("scct",summary[6])
          self.publish("scnu",summary[18])
          self.publish("scnt",summary[20])

    try:
      proc = subprocess.Popen(['/usr/local/bin/showq',
      '-s',
      '-p',
      'seas_gpu',
      ], stdout=subprocess.PIPE,
      universal_newlines=True)
    except:
      exit
    else:
      for line in proc.stdout:
        if "cores" in line:
          line = line.replace("("," ").replace(")"," ")
          summary = (" ".join(line.split())).split(" ")
          # Publishes number of used cores, total cores, used nodes, and total nodes in the seas gpu partition
          self.publish("sgcu",summary[4])
          self.publish("sgct",summary[6])
          self.publish("sggu",summary[11])
          self.publish("sggt",summary[13])
          self.publish("sgnu",summary[18])
          self.publish("sgnt",summary[20])

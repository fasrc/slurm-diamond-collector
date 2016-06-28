#!/usr/bin/python

"""
slurm_cluster_status_diamond.py
A script that uses PySlurm to get general slurm cluster statistics.
"""

import sys,os,json,subprocess,shlex
import diamond.collector

class SlurmClusterStatusCollector(diamond.collector.Collector):
	def get_default_config(self):
		"""
		Returns the default collector settings
		"""
		config = super(SlurmClusterStatusCollector, self).get_default_config()
		config.update({
		'path':     'lsload'
		})
		return config

	def collect(self):

		try:
			proc = subprocess.Popen([
			'scontrol',
			'-o', 'show', 'node'
			], stdout=subprocess.PIPE)
		except:
			return
		else:

			#Zero out counters
			CPUTot=0
			CPULoad=0
			CPUAlloc=0
			RealMem=0
			MemAlloc=0
			MemLoad=0
			NodeTot=0
			IDLETot=0
			DOWNTot=0
			DRAINTot=0
			MIXEDTot=0
			ALLOCTot=0
			RESTot=0
			COMPTot=0
			IDLECPU=0
			MIXEDCPU=0
			ALLOCCPU=0
			COMPCPU=0
			RESCPU=0
			DRAINCPU=0
			DOWNCPU=0
			IDLEMem=0
			MIXEDMem=0
			ALLOCMem=0
			COMPMem=0
			DRAINMem=0
			DOWNMem=0
			RESMem=0
			PerAlloc=0

			#Cycle through each node
			for line in proc.stdout:
				#Turn node information into a hash
				node = dict(s.split("=") for s in shlex.split(line) if '=' in s)

				#Counters.
				NodeTot=NodeTot+1
				CPUTot=CPUTot+int(node['CPUTot'])
				CPUAlloc=CPUAlloc+int(node['CPUAlloc'])
				if node['CPULoad'] != 'N/A':
					CPULoad=CPULoad+float(node['CPULoad'])
				RealMem=RealMem+int(node['RealMemory'])
				MemAlloc=RealMem+int(node['AllocMem'])
				#Slurm only lists actual free memory so we have to back calculate how much is actually used.
				if node['FreeMem'] != 'N/A':
					MemLoad=MemLoad+(int(node['RealMemory'])-int(node['FreeMem']))

				#Count how many nodes are in each state
				if node['State'] == 'IDLE' or node['State'] == 'IDLE+COMPLETING':
					IDLETot=IDLETot+1
					IDLECPU=IDLECPU+int(node['CPUTot'])
					IDLEMem=IDLEMem+int(node['RealMemory'])
				if node['State'] == 'MIXED' or node['State'] == 'MIXED+COMPLETING':
					MIXEDTot=MIXEDTot+1
					MIXEDCPU=MIXEDCPU+int(node['CPUTot'])
					MIXEDMem=MIXEDMem+int(node['RealMemory'])
				if node['State'] == 'ALLOCATED' or node['State'] == 'ALLOCATED+COMPLETING':
					ALLOCTot=ALLOCTot+1
					ALLOCCPU=ALLOCCPU+int(node['CPUTot'])
					ALLOCMem=ALLOCMem+int(node['RealMemory'])
				if "RESERVED" in node['State']:
					RESTot=RESTot+1
					RESCPU=RESCPU+int(node['CPUTot'])
					RESMem=RESMem+int(node['RealMemory'])
				if "COMPLETING" in node['State']:
					COMPTot=COMPTot+1
					COMPCPU=COMPCPU+int(node['CPUTot'])
					COMPMem=COMPMem+int(node['RealMemory'])
				if "DRAIN" in node['State'] and node['State'] != 'IDLE+DRAIN' and node['State'] != 'DOWN+DRAIN':
					DRAINTot=DRAINTot+1
					DRAINCPU=DRAINCPU+int(node['CPUTot'])
					DRAINMem=DRAINMem+int(node['RealMemory'])
				if "DOWN" in node['State'] or node['State'] == 'IDLE+DRAIN':
					DOWNTot=DOWNTot+1
					DOWNCPU=DOWNCPU+int(node['CPUTot'])
					DOWNMem=DOWNMem+int(node['RealMemory'])

				#Calculate percent occupation of all nodes.  Some nodes may have few cores used but all their memory allocated.
				#Thus the node is fully used even though it is not labelled Alloc.  This metric is an attempt to count this properly.
				PerAlloc=PerAlloc+max(float(node['CPUAlloc'])/float(node['CPUTot']),float(node['AllocMem'])/float(node['RealMemory']))

			#Ship it.
			self.publish("nodetot",NodeTot)
			self.publish("cputot",CPUTot)
			self.publish("cpualloc",CPUAlloc)
			self.publish("cpuload",CPULoad)
			self.publish("realmem",RealMem)
			self.publish("memalloc",MemAlloc)
			self.publish("memload",MemLoad)
			self.publish("idletot",IDLETot)
			self.publish("downtot",DOWNTot)
			self.publish("draintot",DRAINTot)
			self.publish("mixedtot",MIXEDTot)
			self.publish("alloctot",ALLOCTot)
			self.publish("comptot",COMPTot)
			self.publish("restot",RESTot)
			self.publish("idlecpu",IDLECPU)
			self.publish("downcpu",DOWNCPU)
			self.publish("draincpu",DRAINCPU)
			self.publish("mixedcpu",MIXEDCPU)
			self.publish("alloccpu",ALLOCCPU)
			self.publish("compcpu",COMPCPU)
			self.publish("rescpu",RESCPU)
			self.publish("idlemem",IDLEMem)
			self.publish("downmem",DOWNMem)
			self.publish("drainmem",DRAINMem)
			self.publish("mixedmem",MIXEDMem)
			self.publish("allocmem",ALLOCMem)
			self.publish("compmem",COMPMem)
			self.publish("resmem",RESMem)
			self.publish("peralloc",PerAlloc)

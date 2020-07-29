#!/usr/bin/python

"""
slurm_cluster_status_diamond.py
A script to get general slurm cluster statistics.
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
			GPUTot=0
			GPUAlloc=0
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
			IDLEGPU=0
			MIXEDGPU=0
			ALLOCGPU=0
			COMPGPU=0
			DRAINGPU=0
			DOWNGPU=0
			RESGPU=0
			PerAlloc=0

			#Cycle through each node
			for line in proc.stdout:
				#Turn node information into a hash
				node = dict(s.split("=", 1) for s in shlex.split(line) if '=' in s)

				#Break out TRES so we can get GPU info.
        			cfgtres = dict(s.split("=", 1) for s in shlex.split(node['CfgTRES'].replace(",", " ")) if '=' in s)
        			alloctres = dict(s.split("=", 1) for s in shlex.split(node['AllocTRES'].replace(",", " ")) if '=' in s)

				#Test for GPU
				if 'gres/gpu' in cfgtres:
					numgpu=int(cfgtres['gres/gpu'])
					if 'gres/gpu' in alloctres:
						agpu=int(alloctres['gres/gpu'])
					else:
						agpu=0
				else:
					numgpu=0
					agpu=0

				#Counters.
				NodeTot=NodeTot+1
				CPUTot=CPUTot+int(node['CPUTot'])
				CPUAlloc=CPUAlloc+int(node['CPUAlloc'])
				if node['CPULoad'] != 'N/A':
					CPULoad=CPULoad+float(node['CPULoad'])
				RealMem=RealMem+int(node['RealMemory'])
				MemAlloc=MemAlloc+min(int(node['AllocMem']),int(node['RealMemory']))
				#Slurm only lists actual free memory so we have to back calculate how much is actually used.
				if node['FreeMem'] != 'N/A':
					MemLoad=MemLoad+(int(node['RealMemory'])-int(node['FreeMem']))

				GPUTot=GPUTot+numgpu
				GPUAlloc=GPUAlloc+agpu

				#Count how many nodes are in each state
				if node['State'] == 'IDLE' or node['State'] == 'IDLE+COMPLETING' or node['State'] == 'IDLE+POWER' or node['State'] == 'IDLE#':
					IDLETot=IDLETot+1
					IDLECPU=IDLECPU+int(node['CPUTot'])
					IDLEMem=IDLEMem+int(node['RealMemory'])
					IDLEGPU=IDLEGPU+numgpu
				if node['State'] == 'MIXED' or node['State'] == 'MIXED+COMPLETING' or node['State'] == 'MIXED#':
					MIXEDTot=MIXEDTot+1
					MIXEDCPU=MIXEDCPU+int(node['CPUTot'])
					MIXEDMem=MIXEDMem+int(node['RealMemory'])
					MIXEDGPU=MIXEDGPU+numgpu
				if node['State'] == 'ALLOCATED' or node['State'] == 'ALLOCATED+COMPLETING':
					ALLOCTot=ALLOCTot+1
					ALLOCCPU=ALLOCCPU+int(node['CPUTot'])
					ALLOCMem=ALLOCMem+int(node['RealMemory'])
					ALLOCGPU=ALLOCGPU+numgpu
				if "RESERVED" in node['State']:
					RESTot=RESTot+1
					RESCPU=RESCPU+int(node['CPUTot'])
					RESMem=RESMem+int(node['RealMemory'])
					RESGPU=RESGPU+numgpu
				if "COMPLETING" in node['State']:
					COMPTot=COMPTot+1
					COMPCPU=COMPCPU+int(node['CPUTot'])
					COMPMem=COMPMem+int(node['RealMemory'])
					COMPGPU=COMPGPU+numgpu
				if "DRAIN" in node['State'] and node['State'] != 'IDLE+DRAIN' and node['State'] != 'DOWN+DRAIN':
					DRAINTot=DRAINTot+1
					DRAINCPU=DRAINCPU+int(node['CPUTot'])
					DRAINMem=DRAINMem+int(node['RealMemory'])
					DRAINGPU=DRAINGPU+numgpu
				if "DOWN" in node['State'] or node['State'] == 'IDLE+DRAIN':
					DOWNTot=DOWNTot+1
					DOWNCPU=DOWNCPU+int(node['CPUTot'])
					DOWNMem=DOWNMem+int(node['RealMemory'])
					DOWNGPU=DOWNGPU+numgpu

				#Calculate percent occupation of all nodes.  Some nodes may have few cores used but all their memory allocated.
				#Thus the node is fully used even though it is not labelled Alloc.  This metric is an attempt to count this properly.
				#Similarly if all the GPU's on a gpu node are used it is fully utilized even though CPU and Mem may still be available.
				PerAlloc=PerAlloc+max(float(node['CPUAlloc'])/float(node['CPUTot']),min(float(node['AllocMem']),float(node['RealMemory']))/float(node['RealMemory']),float(agpu)/max(1,float(numgpu)))

			#Ship it.
			self.publish("nodetot",NodeTot)
			self.publish("cputot",CPUTot)
			self.publish("cpualloc",CPUAlloc)
			self.publish("cpuload",CPULoad,precision=2)
			self.publish("realmem",RealMem)
			self.publish("memalloc",MemAlloc)
			self.publish("memload",MemLoad)
			self.publish("gputot",GPUTot)
			self.publish("gpualloc",GPUAlloc)
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
                        self.publish("idlegpu",IDLEGPU)
                        self.publish("downgpu",DOWNGPU)
                        self.publish("draingpu",DRAINGPU)
                        self.publish("mixedgpu",MIXEDGPU)
                        self.publish("allocgpu",ALLOCGPU)
                        self.publish("compgpu",COMPGPU)
                        self.publish("resgpu",RESGPU)
			self.publish("peralloc",PerAlloc,precision=2)

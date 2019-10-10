#!/usr/bin/python
"""
slurm_job_waste.py

Who is requesting more resources than they need?
"""
import re
import subprocess
from datetime import datetime

import diamond.collector

class SlurmJobWasteCollector(diamond.collector.Collector):

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(SlurmJobWasteCollector, self).get_default_config()
        config.update({
            'path': 'waste'
        })
        return config

    def convert2sec(self, t):
        #Converts DD-HH:MM:SS to seconds
        if ('-' in t):
            (days, tempt) = t.split('-')
	    else:
	        tempt = t
	        days = 0

        if (tempt.count(":") == 2):
            (hours, mins, secs) = tempt.split(':')
        elif(tempt.count(":") == 1):
            (mins,secs) = tempt.split(':')
            hours = 0
        else:
        	secs = tempt
        	mins = 0
            hours = 0
        tsec = 86400.0*float(days)+3600.0*float(hours)+60.0*float(mins)+float(secs)
        return tsec

    def collect(self):
        # Initial settings
        CPUTRESWeight = 1.0
        MemTRESWeight = 0.25
        CPUWastedTRES = 0
        MemWastedTRES = 0
        User = ""
        memstats = {}
        cpustats = {}
        account = {}

        """
        Collect job waste per user
        """
        try:
            # Grab data from slurm
            proc = subprocess.Popen(
	                'sacct -S {hour}:00 -E now --state=CD --units=G -n -P -o user,Account,ReqCPUS,NNodes,ReqMEM,MaxRSS,Elapsed,TotalCPU'.format(
                        hour=datetime.now().hour
                    ).split(),
                    stdout=subprocess.PIPE
            )
        except Exception:
            self.log.exception("error occured fetching job hash")
            return
        else:
            for line in proc.stdout:
                # Split out data
	            (LUser, LAccount, LReqCPUS, LNNodes, LReqMem, LMaxRSS, LElapsed, LTotalCPU) = line.strip().split('|')
	            if LUser != "":
                    # If on initial job entry, pull majority of data
		            if User != "":
                        # If this the start of a new Job permanently store data
			            cpustats[User] = float(cpustats[User])+CPUWastedTRES
			            memstats[User] = float(memstats[User])+MemWastedTRES

                # Data
		        User = LUser
		        if User in memstats:
	                continue
		        else:
		            memstats[User]=0

        		if User in cpustats:
        	        continue
		        else:
		            cpustats[User]=0

		        account[User]=LAccount
		        ReqCPUS = LReqCPUS
		        NNodes = LNNodes
		        ReqMem = LReqMem
		        Elapsed = LElapsed
		        TotalCPU = LTotalCPU
		        MaxRSS = 0

                if ('Gn' in ReqMem):
                    ReqMem=ReqMem.strip("Gn")
                if ('Gc' in ReqMem):
                    ReqMem=float(ReqMem.strip("Gc"))*float(ReqCPUS)/float(NNodes)

		        # Now to compute CPU Wasted Tres
                elapsedt = self.convert2sec(Elapsed)

		        totalcput = self.convert2sec(TotalCPU))

		        CPUWastedTRES=max(0,CPUTRESWeight*(float(ReqCPUS)*float(elapsedt)-float(totalcput)))
	        else:
		        MaxRSS=max(MaxRSS,LMaxRSS.strip("G"))

		        # Now to compute Mem Wasted Tres
		        MemWastedTRES=max(0,MemTRESWeight*(float(ReqMem)-float(MaxRSS))*float(elapsedt))		

            cpustats[User] = float(cpustats[User])+CPUWastedTRES
            memstats[User] = float(memstats[User])+MemWastedTRES

            # Publish data
            for user in account[user]:
                totalwaste = float(cpustats[user]) + float(memstats[user])
                self.publish('{}.{}.cpuwaste'.format(user,account[user]),cpustats[user],precision=6)
                self.publish('{}.{}.memwaste'.format(user,account[user]),memstats[user],precision=6)
                self.publish('{}.{}.totalwaste'.format(user,account[user]),totalwaste,precision=6)

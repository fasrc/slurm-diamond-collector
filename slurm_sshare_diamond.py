#!/usr/bin/python

"""
slurm_sshare_diamond.py
A script that uses PySlurm to get the slurm sshare statistics.
"""

import sys,os,json,subprocess
import diamond.collector

class SlurmSshareCollector(diamond.collector.Collector):
	def get_default_config(self):
		"""
		Returns the default collector settings
		"""
		config = super(SlurmSshareCollector, self).get_default_config()
		config.update({
		'path':     'sshare'
		})
		return config

	def collect(self):

		try:
	        	# sshare command we will use to get the data
			proc = subprocess.Popen([
			'sshare',
			'-ahP', '--format=User,Account,RawShares,NormShares,RawUsage,NormUsage,Fairshare'
			], stdout=subprocess.PIPE)
		except:
			return
		else:
			for line in proc.stdout:
				(User, Account, RawShares, NormShares, RawUsage, NormUsage, Fairshare) = line.strip().split('|')
				Account=Account.replace(" ","")
				User=User.replace(" ","")
				# Need to deal with users that are set to parent for their Shares.
				if User == "" and Account:
					RawSharesAccount=RawShares
				if RawShares == 'parent':
					RawShares=RawSharesAccount
				if NormShares == "":
					NormShares=0
				if User and Account:
					self.publish('{}.{}.rawshares'.format(Account,User),RawShares)
					self.publish('{}.{}.normshares'.format(Account,User),NormShares,precision=6)
					self.publish('{}.{}.rawusage'.format(Account,User),RawUsage)
					self.publish('{}.{}.normusage'.format(Account,User),NormUsage,precision=6)
					self.publish('{}.{}.fairshare'.format(Account,User),Fairshare,precision=6)

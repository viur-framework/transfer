# -*- coding: utf-8 -*-
from plugins import Plugin
import json
from gkey.key import Key
from datetime import datetime

class RewriteDates( Plugin ):
	"""
		Plugin to fix realy old backups (where dates had been converted to string).
		Useless on any recent backup
	"""


	def __init__(self, *args, **kwargs):
		super( RewriteDates, self).__init__(*args, **kwargs)
		self.cfg = None

	def handleStructureResponse(self, structIn ):
		self.cfg = structIn
		return( structIn )

	def handleEntryResponse(self, modul, entryIn ):
		assert self.cfg is not None
		appID = self.sink.getAppId()
		if not modul in self.cfg.keys():
			print("RewriteDates cannot process a modul it doesn't know anything about: %s" % modul)
			return( modul, entryIn )
		modulCfg = self.cfg[ modul ]
		for boneName, boneInfo in modulCfg:
			if "type" in boneInfo.keys() and boneInfo["type"]=="date":
				if boneName in entryIn.keys() and entryIn[boneName]:
					data = entryIn[boneName]
					if isinstance( data, unicode ) and data!="None":
						try:
							data = datetime.strptime(str( data ), "%d.%m.%Y %H:%M:%S")
						except:
							pass
						entryIn[boneName] = data
		return( modul, entryIn )
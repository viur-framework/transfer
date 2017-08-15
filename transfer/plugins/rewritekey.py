# -*- coding: utf-8 -*-
from plugins import Plugin
from gkey.key import Key
from utils import rewriteKey

class RewriteKey(Plugin):
	"""
		Fixes the id (key) property to reflect the changed _appid
		Can be left out if you reimport data backuped from the same application.
	"""


	def handleEntryResponse(self, modul, entryIn ):
		if not entryIn:
			return(modul, entryIn)
		appID = self.sink.getAppId()
		if appID is None:
			return( modul, entryIn )
		key = Key(encoded=entryIn["id"])
		newKey = rewriteKey(key, appID)
		entryIn["id"] = str(newKey)
		return( modul, entryIn )

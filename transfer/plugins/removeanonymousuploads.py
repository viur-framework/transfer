# -*- coding: utf-8 -*-
from plugins import Plugin
import json
from transfer import StopPropagationException

class RemoveAnonymousUploads( Plugin ):
	"""
		Removes all anonymous uploads
		Probably not what you want
	"""
	def __init__(self, *args, **kwargs):
		super( RemoveAnonymousUploads, self).__init__(*args, **kwargs)
		self.cfg = None

	def handleStructureResponse(self, structIn ):
		self.cfg = structIn
		return( structIn )

	def handleEntryResponse(self, module, entryIn ):
		assert self.cfg is not None
		appID = self.sink.getAppId()
		if module!="files":
			return module, entryIn
		if not "parentdir" in entryIn.keys() or not entryIn["parentdir"]:
			raise StopPropagationException()
		return module, entryIn
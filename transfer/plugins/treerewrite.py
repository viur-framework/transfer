# -*- coding: utf-8 -*-
from plugins import Plugin
from gkey.key import Key
import json
import pprint, sys
from itertools import chain

class TreeRewrite( Plugin ):
	"""
		This fixes references to our parent directory / rootNone.
		Currently, you must name all tree modules in your job.json by hand.
		If you forget to name an tree module here, the transferred data will be broken.
	"""
	def __init__(self, *args, **kwargs):
		super( TreeRewrite, self).__init__(*args, **kwargs)
		self.blobMap = {}
		myName = self.__class__.__name__
		if not myName in self.cfg["plugin-cfg"] or not "modules" in self.cfg["plugin-cfg"][myName]:
			raise ValueError("You must set a list of modules in \"plugin-cfg\" for %s" % myName)
		self.rewriteKinds = list(chain(*[("%s" % x, "%s_rootNode" % x) for x in self.cfg["plugin-cfg"][myName]["modules"]]))

	def handleStructureResponse(self, structIn ):
		self.structIn = structIn
		return( structIn )

	def handleEntryResponse(self, modul, entryIn ):
		if not entryIn:
			return(modul, entryIn)
		if Key(encoded=entryIn["id"]).kind() in self.rewriteKinds:
			appID = self.sink.getAppId()
			#Rewrite parentDir
			if "parentdir" in entryIn.keys() and entryIn["parentdir"]:
				try:
					key = Key(encoded=entryIn["parentdir"])
					parent = None
					if key.parent():
						parent = Key.from_path(key.parent().kind(), key.parent().id_or_name(), parent=None, _app=appID)
					newKey = Key.from_path(key.kind(), key.id_or_name(), parent=parent, _app=appID)
					entryIn["parentdir"] = str(newKey)
				except:
					pass
			#Rewrite parentRepo
			if "parentrepo" in entryIn.keys() and entryIn["parentrepo"]:
				try:
					key = Key(encoded=entryIn["parentrepo"])
					parent = None
					if key.parent():
						parent = Key.from_path(key.parent().kind(), key.parent().id_or_name(), parent=None, _app=appID)
					newKey = Key.from_path(key.kind(), key.id_or_name(), parent=parent, _app=appID)
					entryIn["parentrepo"] = str(newKey)
				except:
					pass
		return( modul, entryIn )

# -*- coding: utf-8 -*-
from plugins import Plugin
import json
from gkey.key import Key
import pprint
from utils import rewriteKey

class RewriteRelations(Plugin):
	"""
		Fixes the key properties in relations to reflect the changed appid encoded within the key
	"""

	def __init__(self, *args, **kwargs):
		super( RewriteRelations, self).__init__(*args, **kwargs)
		self.structIn = None

	def handleStructureResponse(self, structIn ):
		self.structIn = structIn
		return( structIn )

	def handleEntryResponse(self, modul, entryIn ):
		assert self.cfg is not None

		if not modul in self.structIn.keys():
			print("RewriteRelations cannot process a modul it doesn't know anything about: %s" % modul)
			return( modul, entryIn )

		if self.cfg["viur.version"] == 1:
			return self.handleEntryResponseV1(modul, entryIn)
		if self.cfg["viur.version"] == 2:
			return self.handleEntryResponseV2(modul, entryIn)
		else:
			raise NotImplementedError("Unknown ViUR Version")

	def handleEntryResponseV2(self, modul, entryIn ):
		modulCfg = self.structIn[ modul ]
		appID = self.sink.getAppId()
		for boneName, boneInfo in modulCfg:
			if not ( "type" in boneInfo.keys()
			         and ( boneInfo["type"].startswith("treeitem")
			               or boneInfo["type"].startswith("relational")
			               or boneInfo["type"].startswith("hierarchy") )
					 and boneName in entryIn.keys() and entryIn[boneName] ):
				continue

			data = entryIn[ boneName ]

			if isinstance(data,str) or isinstance(data,unicode):
				try:
					data = json.loads( data )
				except:
					print("Ignoring garbarge in %s/%s" % (modul,boneName))
					continue

				if isinstance( data, dict ) and "dest" in data and "key" in data["dest"]:
					print(data["dest"]["key"])
					key = Key(encoded=data["dest"]["key"])
					data["dest"]["key"] = str(rewriteKey(key, appID))
					data = json.dumps( data )
					entryIn[boneName] = data
				elif isinstance( data, list):
					tmpRes = []
					for d in data:
						if isinstance(d,dict) and "dest" in d and "key" in d["dest"]:
							key = Key(encoded=d["dest"]["key"])
							d["dest"]["key"] = str(rewriteKey(key, appID))
							tmpRes.append(d)
					data = [json.dumps( x ) for x in tmpRes ]
					entryIn[boneName] = data
				elif isinstance(data,list):
					tmpRes = []
					for x in data:
						try:
							newVal = json.loads(x)
						except:
							print("Ignoring garbarge in %s/%s" % (modul,boneName))
							continue
						if isinstance(newVal, dict) and "dest" in newVal and "key" in newVal["dest"]:
							key = Key(encoded=newVal["dest"]["key"])
							newVal["dest"]["key"] = str(rewriteKey(key, appID))
							tmpRes.append(newVal)
						elif isinstance( newVal, list):
							for d in newVal:
								if isinstance(d,dict) and "dest" in d and "key" in d["dest"]:
									key = Key(encoded=d["dest"]["key"])
									d["dest"]["key"] = str(rewriteKey(key, appID))
									tmpRes.append(d)
					newVal = [json.dumps( x ) for x in tmpRes]
					entryIn[boneName] = newVal
		return( modul, entryIn )

	def handleEntryResponseV1(self, modul, entryIn):
		modulCfg = self.cfg[modul]
		appID = self.sink.getAppId()
		for boneName, boneInfo in modulCfg:
			if not ( "type" in boneInfo.keys()
			         and ( boneInfo["type"] == "treeitem.file"
			               or boneInfo["type"].startswith("relational")
			               or boneInfo["type"].startswith("extendedrelational")
			               or boneInfo["type"].startswith("hierarchy") )
					 and boneName in entryIn.keys() and entryIn[boneName] ):
				continue

			if boneInfo["type"].startswith("extendedrelational"):
				if not boneInfo[ "multiple" ]:
					rels = [ entryIn[ boneName ] ]
				else:
					rels = entryIn[ boneName ]

				rrels = []
				for rel in rels:
					rel = json.loads( rel )

					if boneInfo[ "type" ].startswith( "extendedrelational" ):
						trel = rel[ "dest" ]
					else:
						trel = rel

					if "id" in trel.keys():
						key = Key( encoded=trel["id"] )
						trel["id"] = str( Key.from_path( key.kind(), key.id_or_name(), parent=key.parent(), _app=appID) )

					if boneInfo[ "type" ].startswith( "extendedrelational" ):
						rel[ "dest" ] = trel
						rrels.append( json.dumps( rel ) )
					else:
						rrels.append( json.dumps( trel ) )

				if not boneInfo[ "multiple" ]:
					entryIn[ boneName ] = rrels[0]
				else:
					entryIn[ boneName ] = rrels

			else:
				data = entryIn[ boneName ]

				if isinstance(data,str) or isinstance(data,unicode):
					try:
						data = json.loads( data )
					except:
						print("Ignoring garbarge in %s/%s" % (modul,boneName))
						continue

					if isinstance( data, dict ) and "id" in data.keys():
						key = Key(encoded=data["id"])
						data["id"] = str( Key.from_path(key.kind(), key.id_or_name(), parent=key.parent(), _app=appID))
						data = json.dumps( data )
						entryIn[boneName] = data
					elif isinstance( data, list):
						tmpRes = []
						for d in data:
							if isinstance(d,dict) and "id" in d.keys():
								key = Key(encoded=d["id"])
								d["id"] = str( Key.from_path(key.kind(), key.id_or_name(), parent=key.parent(), _app=appID))
								tmpRes.append(d)
						data = [json.dumps( x ) for x in tmpRes ]
						entryIn[boneName] = data

				elif isinstance(data,list):
					tmpRes = []
					for x in data:
						try:
							newVal = json.loads( x )
						except:
							print("Ignoring garbarge in %s/%s" % (modul,boneName))
							continue
						if isinstance( newVal, dict ) and "id" in newVal.keys():
							key = Key(encoded=newVal["id"])
							newVal["id"] = str( Key.from_path(key.kind(), key.id_or_name(), parent=key.parent(), _app=appID))
							tmpRes.append( newVal )
						elif isinstance( newVal, list):
							for d in newVal:
								if isinstance(d,dict) and "id" in d.keys():
									key = Key(encoded=d["id"])
									d["id"] = str( Key.from_path(key.kind(), key.id_or_name(), parent=key.parent(), _app=appID))
									tmpRes.append(d)

					newVal = [json.dumps( x ) for x in tmpRes ]
					entryIn[boneName] = newVal

				else:
					print("Ignoring garbarge in %s/%s" % (modul,boneName))
					print( data, type(data) )
					continue

		return( modul, entryIn )

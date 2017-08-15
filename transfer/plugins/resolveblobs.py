# -*- coding: utf-8 -*-
from plugins import Plugin
import json
from transfer import StopPropagationException

class ResolveBlobs( Plugin ):
	"""
		This checks each entry for fileBones, uploads the referenced blob and updates the values
		of these relations accordingly. Remove this plugin if you only want to transfer entries from
		the datastore, not any referenced blobs.
	"""
	def __init__(self, *args, **kwargs):
		super( ResolveBlobs, self).__init__(*args, **kwargs)
		self.blobMap = {}

	def handleStructureResponse(self, structIn ):
		self.structIn = structIn
		return( structIn )

	def handleEntryResponse(self, modul, entryIn ):
		assert self.structIn is not None
		
		if not entryIn:
			return( modul, entryIn )

		if not modul in self.structIn.keys():
			print("ResolveBlobs cannot process a modul it doesn't know anything about: %s" % modul)
			return( modul, entryIn )
		#Check for referenced Files (as in fileBone)
		if self.cfg["viur.version"] == 1:
			return self.handleEntryResponseV1(modul, entryIn)
		if self.cfg["viur.version"] == 2:
			return self.handleEntryResponseV2(modul, entryIn)
		else:
			raise NotImplementedError("Unknown ViUR Version")

	def handleEntryResponseV2(self, modul, entryIn):
		modulCfg = self.structIn[ modul ]
		for boneName, boneInfo in modulCfg:
			if "type" in boneInfo.keys() and boneInfo["type"]=="treeitem.file":
				if boneName in entryIn.keys() and entryIn[boneName]:
					data = entryIn[boneName]
					if isinstance( data, list):
						tmpRes = []
						for d in data:
							d = json.loads( d )
							if isinstance(d,dict) and "dest" in d and "dlkey" in d["dest"]:
								key = d["dest"]["dlkey"]
								if not key in self.blobMap.keys():
									try:
										self.qp.requestBlob( self, key)
									except StopPropagationException:
										pass
								if not key in self.blobMap.keys():
									print("Ive missed a blob?!")
									continue
								d["dest"]["dlkey"] = self.blobMap[key]

								# Fileeintrag holen
								if d["dest"]["key"]!=entryIn["id"]:
									try:
										self.qp.requestEntry( self, "file", d["dest"]["key"] )
									except StopPropagationException:
										pass

								tmpRes.append(d)
						data = [json.dumps( x ) for x in tmpRes]
					else:
						try:
							data = json.loads( data )
						except:
							print("Ignoring garbarge in %s/%s" % (modul,boneName))
							continue
						if isinstance( data, dict ) and "dest" in data and "dlkey" in data["dest"]:
							key = data["dest"]["dlkey"]
							if not key in self.blobMap.keys():
								try:
									self.qp.requestBlob(self, key)
								except StopPropagationException:
									pass
							if not key in self.blobMap.keys():
								print("Ive missed a blob?!")
								continue
							data["dest"]["dlkey"] = self.blobMap[ key ]

							# Fileeintrag holen
							if data["dest"]["key"] != entryIn["id"]:
								try:
									self.qp.requestEntry( self, "file", data["dest"]["key"] )
								except StopPropagationException:
									pass

							data = json.dumps( data )
						elif isinstance( data, list):
							tmpRes = []
							for d in data:
								if isinstance(d,dict) and "dest" in d and "dlkey" in d["dest"]:
									key = d["dest"]["dlkey"]
									if not key in self.blobMap.keys():
										try:
											self.qp.requestBlob(self, key)
										except StopPropagationException:
											pass
									if not key in self.blobMap.keys():
										print("Ive missed a blob?!")
										continue
									d["dest"]["dlkey"] = self.blobMap[key]

									# Fileeintrag holen
									if d["dest"]["key"] != entryIn["id"]:
										try:
											self.qp.requestEntry( self, "file", d["dest"]["key"] )
										except StopPropagationException:
											pass

									tmpRes.append(d)
							data = json.dumps( tmpRes )

					entryIn[boneName] = data
		#Test if this is an entry from the file module
		if modul=="file" and "dlkey" in entryIn.keys():
			key = entryIn["dlkey"]
			if not key in self.blobMap.keys():
				try:
					self.qp.requestBlob( self, key )
				except StopPropagationException:
					pass
			if key in self.blobMap.keys():
				entryIn["dlkey"] = self.blobMap[ key ]
			else:
				print("Unknown blobkey: %s" % key)
		return(modul, entryIn)


	def handleEntryResponseV1(self, modul, entryIn):
		modulCfg = self.structIn[ modul ]
		for boneName, boneInfo in modulCfg:
			if "type" in boneInfo.keys() and boneInfo["type"]=="treeitem.file":
				if boneName in entryIn.keys() and entryIn[boneName]:
					data = entryIn[boneName]
					if isinstance( data, list):
						tmpRes = []
						for d in data:
							d = json.loads( d )
							if isinstance(d,dict) and "dlkey" in d.keys():
								key = d["dlkey"]
								if not key in self.blobMap.keys():
									try:
										self.qp.requestBlob( self, key)
									except StopPropagationException:
										pass
								if not key in self.blobMap.keys():
									print("Ive missed a blob1?!")
									continue
								d["dlkey"] = self.blobMap[key]

								# Fileeintrag holen
								if d["id"]!=entryIn["id"]:
									try:
										self.qp.requestEntry( self, "file", d["id"] )
									except StopPropagationException:
										pass

								tmpRes.append(d)
						data = [json.dumps( x ) for x in tmpRes]
					else:
						try:
							data = json.loads( data )
						except:
							print("Ignoring garbarge in %s/%s" % (modul,boneName))
							continue
						if isinstance( data, dict ) and "dlkey" in data.keys():
							key = data["dlkey"]
							if not key in self.blobMap.keys():
								try:
									self.qp.requestBlob( self, key)
								except StopPropagationException:
									pass
							if not key in self.blobMap.keys():
								print("Ive missed a blob2?!")
								continue
							data["dlkey"] = self.blobMap[ key ]

							# Fileeintrag holen
							if data["id"] != entryIn["id"]:
								try:
									self.qp.requestEntry( self, "file", data["id"] )
								except StopPropagationException:
									pass

							data = json.dumps( data )
						elif isinstance( data, list):
							tmpRes = []
							for d in data:
								if isinstance(d,dict) and "dlkey" in d.keys():
									key = d["dlkey"]
									if not key in self.blobMap.keys():
										try:
											self.qp.requestBlob( self, key)
										except StopPropagationException:
											pass
									if not key in self.blobMap.keys():
										print("Ive missed a blob?!")
										continue
									d["dlkey"] = self.blobMap[key]

									# Fileeintrag holen
									if d["id"] != entryIn["id"]:
										try:
											self.qp.requestEntry( self, "file", d["id"] )
										except StopPropagationException:
											pass

									tmpRes.append(d)
							data = json.dumps( tmpRes )

					entryIn[boneName] = data
		#Test if this is an entry from the file module
		if modul=="file" and "dlkey" in entryIn.keys():
			key = entryIn["dlkey"]
			if not key in self.blobMap.keys():
				try:
					self.qp.requestBlob( self, key )
				except StopPropagationException:
					pass
			if key in self.blobMap.keys():
				entryIn["dlkey"] = self.blobMap[ key ]
			else:
				print("Unknown blobkey: %s" % key)
		return(modul, entryIn)

	def handleBlobStoredResponse( self, key, newKey, blob, mimetype ):
		print("handleBlobStoredResponse", key, newKey, mimetype)
		self.blobMap[ key ] = newKey
		return( key, newKey, blob, mimetype )

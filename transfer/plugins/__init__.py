# -*- coding: utf-8 -*-

import os, logging
import json

class Plugin(object):
	def __init__(self, cfg, source, sink, queueProcessor ):
		super( Plugin, self ).__init__()
		self.cfg = cfg
		self.source = source
		self.sink = sink
		self.qp = queueProcessor

	def handleStructureResponse(self, structIn ):
		"""
			Its possible to rewrite the structure here
			Pipe func: sinks
		"""
		return( structIn )

	def handleEntryResponse(self, modul, entryIn ):
		"""
			Rewrite an entry here (if needed)
			Pipe func: sinks
		"""
		return modul, entryIn

	def handleBlobResponse(self, blobKey, blob ):
		"""
			Pipe func: sinks
		"""
		return( blobKey, blob )

	def handleListRequest(self, modul, ctime ):
		"""
			Pipe func: bubbles
		"""
		return( modul, ctime )

	def handleEntryRequest(self, modul, key):
		"""
			Pipe func: bubbles
		"""
		return( modul, key )

	def handleBlobRequest(self, blobKey ):
		"""
			Pipe func: bubbles
		"""
		return( blobKey )

	def handleBlobStoredResponse( self, key, newKey, blob, mimetype ):
		"""
			Pipe func: bubbles
		"""
		return( key, newKey, blob, mimetype )

	def run(self):
		"""
			Performs the core plugin actions
		"""
		pass

# Auto import plugins
for _module in os.listdir(os.path.dirname(__file__)):

	if _module == "__init__.py" or not _module.endswith(".py"):
		continue

	_module = _module[:-3]

	try:
		_import = __import__(_module, globals(), locals(), [_module])
		for _name in dir(_import):
			if _name.startswith("_"):
				continue

			_symbol = getattr(_import, _name)
			try:
				if issubclass(_symbol, Plugin):
					globals().update({_name: _symbol})
			except TypeError:  # We might see imports of other modules here (where issubclass failes)
				pass
	except:
		logging.error("Unable to import '%s'" % _module)
		raise


# -*- coding: utf-8 -*-
from plugins import Plugin

class TransferEntities( Plugin ):
	"""
		Requests all kinds known to the target application
	"""

	def run( self ):
		assert self.cfg is not None
		transapps = self.structIn.keys()
		for module in transapps:
			print("Processing module %s" % module)
			self.qp.requestList( self, module, 0 )

	def handleStructureResponse(self, structIn ):
		self.structIn = structIn
		return( structIn )

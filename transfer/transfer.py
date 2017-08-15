#!/usr/bin/python2
# -*- coding: utf-8 -*-

import sys
import json
import sources, sinks, plugins
import pprint
from utils import StopPropagationException
import config

class QueueProcessor( object ):
	def __init__( self, cfg, source, sink, plugins ):
		super( QueueProcessor, self ).__init__()
		self.source = source
		self.sink = sink
		self.plugins = [x(cfg, source,sink,self) for x in plugins]

	def run( self ):
		modulCfg = self.sink.listExportModules()
		for p in self.plugins[:]:
			modulCfg = p.handleStructureResponse( modulCfg )
		self.sink.setModulConfig( modulCfg )
		for p in self.plugins[:]:
			p.run()

	def requestList( self, plugin, modul, ctime=0 ):
		assert plugin in self.plugins
		for p in self.plugins[ :self.plugins.index(plugin): -1]:
			modul, ctime = p.handleListRequest( modul, ctime )
		for v in self.source.iterValues( modul, ctime ):
			if v is None:
				continue
			self._sinkEntry( modul, v )

	def requestEntry( self, plugin, modul, key ):
		assert plugin in self.plugins
		for p in self.plugins[ :self.plugins.index(plugin): -1]:
			modul, key = p.handleEntryRequest( modul, key )
		self._sinkEntry( modul, self.source.getEntry(modul, key ) )

	def requestBlob( self, plugin, key ):
		assert plugin in self.plugins
		for p in self.plugins[ :self.plugins.index(plugin): -1]:
			key = p.handleBlobRequest( key )
		try:
			self._sinkBlob( key, *self.source.getBlob( key ) )
		except ValueError:
			return

	def _sinkEntry( self, modul, entry ):
		if not entry:
			return

		try:
			for p in self.plugins[:]:
				modul, entry = p.handleEntryResponse( modul, entry )
			self.sink.storeEntry( modul, entry )
		except Exception as e:
			print("GOT EXCEPT")
			print( e )
			print( type(e ) )
			raise
			if isinstance(e,StopPropagationException):
				return
			else:
				raise
			return


	def _sinkBlob( self, key, blob, mimetype ):
		try:
			for p in self.plugins[:]:
				key, blob = p.handleBlobResponse( key, blob )
			newKey, mimetype = self.sink.storeBlob( key, blob, mimetype )
			for p in self.plugins[ : :-1]:
				key, newKey, blob, mimetype = p.handleBlobStoredResponse( key, newKey, blob, mimetype )
		except StopPropagationException:
			return



def objectsFromModul( modul ):
	res = {}
	for k in dir(modul):
		if k.startswith("_"):
			continue
		v = getattr( modul, k )
		try:
			if issubclass( v, object ):
				res[k] = v
		except TypeError: #Not a class
			pass
	return( res )

if __name__ == "__main__":

	#cfg = json.loads(open("job.json","r").read())
	config.load("job.json")
	avaiableSources = objectsFromModul(sources)
	avaiableSinks = objectsFromModul(sinks)
	avaiablePlugins = objectsFromModul(plugins)
	assert all( [x in config.conf.keys() for x in ["source", "sink","steps", "plugins"] ] ), "Parameter fehlt in job.json"
	if not config.conf["source"] in avaiableSources.keys():
		raise ValueError("Source %s unbekannt. Muss eins aus %s sein." % (config.conf["source"], str(list(avaiableSources.keys()))))
	if not config.conf["sink"] in avaiableSinks.keys():
		raise ValueError("Sink %s unbekannt. Muss eins aus %s sein." % (config.conf["source"], str(list(avaiableSinks.keys()))))
	if not all( [x in avaiablePlugins.keys() for x in config.conf["plugins"]] ):
		raise ValueError("Unbekanntes Plugin. Müssen aus %s sein." % (avaiablePlugins.keys()))
	if not all( [x in ["conf","blob", "data"] for x in config.conf["steps"]] ):
		raise ValueError("Unbekannte Schritte. Müssen conf,blob oder data sein.")

	source = avaiableSources[config.conf["source"]](config.conf)
	sink = avaiableSinks[config.conf["sink"]](config.conf)

	#exportedModules = source.listExportModules()
	#pprint( exportedModules )
	q = QueueProcessor( config.conf, source, sink, [avaiablePlugins[x] for x in config.conf["plugins"]]) #TransferChangedEntrys,ResolveBlobs,RewriteKey,RewriteRelations
	q.run()
	"""
	print("Current Dest values")
	for k in dst.iterValues("organisation"):
		print("dst1", k )
	for m in src.listModules():
		print(m, src.getLastChangeDate(m))
	print("Source values")
	for k in src.iterValues("organisation",0):
		dst.storeEntry("organisation",k)
		print( "src", k )
	print("NEW Dest values")
	for k in dst.iterValues("organisation"):
		print("dst2", k )
	print( "test" )
		"""

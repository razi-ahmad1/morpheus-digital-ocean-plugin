package com.morpheusdata.digitalocean

import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.*
import groovy.util.logging.Slf4j
import com.morpheusdata.core.OptionSourceProvider

@Slf4j
class DigitalOceanOptionSourceProvider implements OptionSourceProvider {

	Plugin plugin
	MorpheusContext morpheusContext

	DigitalOceanOptionSourceProvider(Plugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
	}

	@Override
	MorpheusContext getMorpheus() {
		return this.morpheusContext
	}

	@Override
	Plugin getPlugin() {
		return this.plugin
	}

	@Override
	String getCode() {
		return 'digital-ocean-option-source'
	}

	@Override
	String getName() {
		return 'DigitalOcean Option Source'
	}

	@Override
	List<String> getMethodNames() {
		return new ArrayList<String>(['digitalOceanDataCenters', 'digitalOceanImage'])
	}

	def digitalOceanDataCenters(args) {
		log.debug "datacenters: ${args}"
		return [[value:'nyc1', name:'New York 1', available:true],
		[value:'sfo1', name:'San Francisco 1', available:true],
		[value:'nyc2', name:'New York 2', available:true],
		[value:'ams2', name:'Amsterdam 2', available:true],
		[value:'sgp1', name:'Singapore 1', available:true],
		[value:'lon1', name:'London 1', available:true],
		[value:'nyc3', name:'New York 3', available:true],
		[value:'ams3', name:'Amsterdam 3', available:true],
		[value:'fra1', name:'Frankfurt 1', available:true]]
	}

	def digitalOceanImage(args) {
		log.debug "digitalOceanImage: ${args}"
		def zoneId = args?.size() > 0 ? args.getAt(0).zoneId?.toLong() : null
		def accountId = args?.size() > 0 ? args.getAt(0).accountId?.toLong() : null
		List options = []
		morpheus.virtualImage.listSyncProjectionsByCategory(accountId, "digitalocean.image.os").blockingSubscribe{options << [name: it.name, value: it.id]}
		if(zoneId) {
			morpheus.virtualImage.listSyncProjections(zoneId).blockingSubscribe{options << [name: it.name, value: it.id]}
		}
		return options.unique().sort { it.name }
	}
}

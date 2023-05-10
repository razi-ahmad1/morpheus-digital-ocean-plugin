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
	DigitalOceanApiService apiService

	DigitalOceanOptionSourceProvider(Plugin plugin, MorpheusContext context, DigitalOceanApiService apiService) {
		this.plugin = plugin
		this.morpheusContext = context
		this.apiService = apiService
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
		List datacenters = []
		String apiKey = plugin.getAuthConfig(args.getAt(0) as Map).doApiKey
		log.debug("API KEY: ${apiKey}")
		def response = apiService.listRegions(apiKey)
		if(response.success) {
			datacenters = response.data
		}

		log.debug("listDatacenters regions: $datacenters")
		def rtn = datacenters?.collect { [name: it.name, value: it.slug] }?.sort { it.name } ?: []

		return rtn
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

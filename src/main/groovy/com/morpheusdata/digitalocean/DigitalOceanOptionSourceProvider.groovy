package com.morpheusdata.digitalocean

import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.ReferenceDataSyncProjection
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
		log.debug("datacenters: ${args}")
		List datacenters = []

		// if we know the cloud then load from cached data
		if(args.getAt(0)?.zoneId) {
			Long cloudId = args.getAt(0).zoneId.toLong()
			morpheusContext.referenceData.listByCategory("digitalocean.${cloudId}.datacenter").blockingSubscribe { ReferenceDataSyncProjection refData ->
				datacenters << [name: refData.name, value: refData.externalId]
			}
		}

		// if cloud isn't created or hasn't cached the datacenters yet, load directly from the API
		if(datacenters.size() == 0) {
			log.debug("Datacenters not cached, loading from API")
			DigitalOceanApiService apiService = new DigitalOceanApiService()

			String apiKey = plugin.getAuthConfig(args.getAt(0) as Map).doApiKey
			if(apiKey) {
				def response = apiService.listRegions(apiKey)
				if(response.success) {
					datacenters = response.data?.collect { [name: it.name, value: it.slug] }
				}
			} else {
				log.debug("API key not supplied, failed to load datacenters")
			}

		}

		log.debug("listDatacenters regions: $datacenters")
		def rtn = datacenters?.sort { it.name } ?: []

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

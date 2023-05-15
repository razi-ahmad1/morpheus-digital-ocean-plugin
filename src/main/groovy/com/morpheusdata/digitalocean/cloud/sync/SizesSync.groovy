package com.morpheusdata.digitalocean.cloud.sync

import com.morpheusdata.core.ProvisioningProvider
import com.morpheusdata.model.ProvisionType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.projection.ServicePlanIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.Observable

@Slf4j
class SizesSync {

	private Cloud cloud
	private MorpheusContext morpheusContext
	DigitalOceanApiService apiService
	DigitalOceanPlugin plugin

	SizesSync(DigitalOceanPlugin plugin, Cloud cloud, DigitalOceanApiService apiService) {
		this.plugin = plugin
		this.cloud = cloud
		this.morpheusContext = this.plugin.morpheusContext
		this.apiService = apiService
	}

	def execute() {
		log.debug("SizesSync execute: ${cloud}")
		try {
			String apiKey = plugin.getAuthConfig(cloud).doApiKey
			ServiceResponse response = apiService.listDropletSizes(apiKey)
			List<ServicePlan> servicePlans = []
			response.data?.each {
				def name = getNameForSize(it)
				def servicePlan = new ServicePlan(
						code: "digitalocean.size.${it.slug}",
						provisionTypeCode: 'digitalocean',
						description: name,
						name: name,
						editable: false,
						externalId: it.slug,
						maxCores: it.vcpus,
						maxMemory: it.memory.toLong() * 1024l * 1024l, // MB
						maxStorage: it.disk.toLong() * 1024l * 1024l * 1024l, //GB
						sortOrder: it.disk.toLong(),
						price_monthly: it.price_monthly,
						price_hourly: it.price_hourly,
						refType: 'ComputeZone',
						refId: cloud.id
				)
				servicePlans << servicePlan
			}
			log.debug("api service plans: $servicePlans")
			if (servicePlans) {
				ProvisioningProvider provisioningProvider = this.plugin.getProviderByCode('digitalocean-provision-provider')
				ProvisionType provisionType = new ProvisionType(code: provisioningProvider.provisionTypeCode)
				Observable<ServicePlanIdentityProjection> domainPlans = morpheusContext.servicePlan.listSyncProjections(provisionType)
				SyncTask<ServicePlanIdentityProjection, ServicePlan, ServicePlan> syncTask = new SyncTask(domainPlans, servicePlans)
				syncTask.addMatchFunction { ServicePlanIdentityProjection projection, ServicePlan apiPlan ->
					projection.externalId == apiPlan.externalId
				}.onDelete { List<ServicePlanIdentityProjection> deleteList ->
					morpheusContext.servicePlan.remove(deleteList)
				}.onAdd { createList ->
					while (createList.size() > 0) {
						List chunkedList = createList.take(50)
						createList = createList.drop(50)
						morpheusContext.servicePlan.create(chunkedList).blockingGet()
					}
				}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ServicePlanIdentityProjection, ServicePlan>> updateItems ->

					Map<Long, SyncTask.UpdateItemDto<ServicePlanIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
					morpheusContext.servicePlan.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map {ServicePlan servicePlan ->
						SyncTask.UpdateItemDto<ServicePlanIdentityProjection, Map> matchItem = updateItemMap[servicePlan.id]
						return new SyncTask.UpdateItem<ServicePlan,Map>(existingItem:servicePlan, masterItem:matchItem.masterItem)
					}


				}.onUpdate { updateList ->
					updateMatchedPlans(updateList)
				}.start()


				// sizeList?.each { size ->
				//
				// 	ServicePlan servicePlan = existingServicePlans.find { it.code == "digitalocean.size.${size.slug}" }
				//
				// 	['month', 'hour'].each { priceUnit ->
				// 		def priceSetCode = "digitalocean.size.${size.slug}.${priceUnit}"
				//
				// 		// Get or create the price set
				// 		def name = "${getNameForSize(size)} - ${priceUnit.capitalize()}"
				// 		def priceSet = priceManagerService.getOrCreatePriceSet(
				// 			[code: priceSetCode,
				// 			 name: name,
				// 			 priceUnit: priceUnit,
				// 			 type: AccountPriceSet.PRICE_SET_TYPE.fixed.toString(),
				// 			 systemCreated: true])
				//
				// 		// Get or create the price
				// 		def (price, errors) = priceManagerService.getOrCreatePrice(
				// 			[name: name,
				// 			 code: priceSetCode,
				// 			 priceType: AccountPrice.PRICE_TYPE.fixed,
				// 			 incurCharges: 'always',
				// 			 systemCreated: true,
				// 			 cost: new BigDecimal(size["price_${priceUnit}ly"].toString() ?: '0.0'),
				// 			 priceUnit: priceUnit])
				//
				// 		priceManagerService.addToPriceSet(priceSet, price)
				//
				// 		// Add the set to the correct service plan
				// 		priceManagerService.addPriceSetToPlan(servicePlan, priceSet)
				// 	}
				// }
			}
		} catch(e) {
			log.error("Error in execute : ${e}", e)
		}
	}

	def updateMatchedPlans(List<SyncTask.UpdateItem<ServicePlan,ServicePlan>> updateItems) {
		List<ServicePlan> itemsToUpdate = []
		updateItems.each {it ->
			ServicePlan remoteItem = it.masterItem
			ServicePlan localItem = it.existingItem
			def save = false

			log.debug("updateMatchedPlans: localItem.id: ${localItem.id}, localItem.name: ${localItem.name}, remoteItem.name: ${remoteItem.name}")
			if(localItem.name != remoteItem.name) {
				localItem.name = remoteItem.name
				save = true
			}

			if(save) {
				itemsToUpdate << localItem
			}
		}

		if(itemsToUpdate.size() > 0) {
			log.debug("SizeSync updating ${itemsToUpdate.size()} plans")
			morpheusContext.servicePlan.save(itemsToUpdate).blockingGet()
		}
	}

	def getNameForSize(sizeData) {
		def memoryName = sizeData.memory < 1000 ? "${sizeData.memory} MB" : "${sizeData.memory.div(1024l)} GB"
		return "${sizeData.description} ${sizeData.vcpus} CPU, ${memoryName} Memory, ${sizeData.disk} GB Storage"
	}

	ServiceResponse clean(Cloud cloud, Map opts=[:]) {
		// delete stuff
		return ServiceResponse.success();
	}

}

package com.morpheusdata.digitalocean.cloud.sync

import com.morpheusdata.core.ProvisioningProvider
import com.morpheusdata.core.util.SyncList
import com.morpheusdata.model.Account
import com.morpheusdata.model.AccountPrice
import com.morpheusdata.model.AccountPriceSet
import com.morpheusdata.model.ProvisionType
import com.morpheusdata.model.ServicePlanPriceSet
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.projection.AccountPriceIdentityProjection
import com.morpheusdata.model.projection.AccountPriceSetIdentityProjection
import com.morpheusdata.model.projection.ServicePlanPriceSetIdentityProjection
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

	static final SIZE_PREFIX = "digitalocean.size."

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
						code: "${SIZE_PREFIX}${it.slug}",
						provisionTypeCode: 'digitalocean',
						description: name,
						name: name,
						editable: false,
						externalId: it.slug,
						maxCores: it.vcpus,
						maxMemory: it.memory.toLong() * 1024l * 1024l, // MB
						maxStorage: it.disk.toLong() * 1024l * 1024l * 1024l, //GB
						sortOrder: it.disk.toLong(),
						price_monthly: new BigDecimal((it.price_monthly ?: '0.0').toString()),
						price_hourly: new BigDecimal((it.price_hourly ?: '0.0').toString()),
						refType: 'ComputeZone',
						refId: cloud.id
				)
				servicePlans << servicePlan
			}

			if (servicePlans) {
				ProvisioningProvider provisioningProvider = this.plugin.getProviderByCode('digitalocean-provision-provider')
				ProvisionType provisionType = new ProvisionType(code: provisioningProvider.provisionTypeCode)
				Observable<ServicePlanIdentityProjection> domainPlans = morpheusContext.servicePlan.listSyncProjections(provisionType)
				SyncTask<ServicePlanIdentityProjection, ServicePlan, ServicePlan> syncTask = new SyncTask(domainPlans, servicePlans)
				syncTask.addMatchFunction { ServicePlanIdentityProjection projection, ServicePlan apiPlan ->
					return projection.code == apiPlan.code
				}.onDelete { List<ServicePlanIdentityProjection> deleteList ->
					def deleteIds = deleteList.collect { it.id }
					List<ServicePlanPriceSet> servicePlanPriceSetDeleteList = morpheusContext.servicePlanPriceSet.listByServicePlanIds(deleteIds).toList().blockingGet()
					Boolean servicePlanPriceSetDeleteResult = morpheusContext.servicePlanPriceSet.remove(servicePlanPriceSetDeleteList).blockingGet()
					if(servicePlanPriceSetDeleteResult) {
						morpheusContext.servicePlan.remove(deleteList).blockingGet()
					} else {
						log.error("Failed to delete ServicePlanPriceSets associated to ServicePlans")
					}
				}.onAdd { createList ->
					while (createList.size() > 0) {
						List chunkedList = createList.take(50)
						createList = createList.drop(50)
						Boolean servicePlanCreateSuccess = morpheusContext.servicePlan.create(chunkedList).blockingGet()
						if(servicePlanCreateSuccess) {
							syncPlanPrices(chunkedList)
						}
					}
				}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ServicePlanIdentityProjection, ServicePlan>> updateItems ->
					Map<Long, SyncTask.UpdateItemDto<ServicePlanIdentityProjection, ServicePlan>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
					morpheusContext.servicePlan.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map { ServicePlan servicePlan ->
						SyncTask.UpdateItemDto<ServicePlanIdentityProjection, ServicePlan> matchItem = updateItemMap[servicePlan.id]
						return new SyncTask.UpdateItem<ServicePlan,ServicePlan>(existingItem:servicePlan, masterItem:matchItem.masterItem)
					}
				}.onUpdate { updateList ->
					while (updateList.size() > 0) {
						List chunkedList = updateList.take(50)
						updateList = updateList.drop(50)
						updateMatchedPlans(chunkedList)
					}
				}.start()
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

			if(localItem.name != remoteItem.name) {
				localItem.name = remoteItem.name
				save = true
			}

			if(localItem.price_monthly != remoteItem.price_monthly) {
				localItem.price_monthly = remoteItem.price_monthly
				save = true
			}

			if(save) {
				itemsToUpdate << localItem
			}
		}

		if(itemsToUpdate.size() > 0) {
			morpheusContext.servicePlan.save(itemsToUpdate).blockingGet()
		}

		syncPlanPrices(updateItems.collect { it.existingItem })
	}

	def syncPlanPrices(List<ServicePlan> servicePlans) {

		def priceUnits = ['month', 'hour']
		List<String> priceSetCodes = []
		List<AccountPriceSet> apiPriceSets = []
		List<AccountPrice> apiPrices = []
		Map<String, ServicePlan> priceSetPlans = [:]
		Map<String, ServicePlan> priceSetPrices = [:]

		List<String> servicePlanCodes = servicePlans.collect { it.code }
		Map<String, ServicePlan> tmpServicePlanMap = morpheusContext.servicePlan.listByCode(servicePlanCodes).toList().blockingGet().collectEntries { [(it.code):it]}

		priceUnits.each { String priceUnit ->
			servicePlans.each { ServicePlan servicePlan ->
				def priceSetName = "${servicePlan.name} - ${priceUnit.capitalize()}"
				def priceSetCode = "digitalocean.size.${servicePlan.externalId}.${priceUnit}"
				priceSetCodes << priceSetCode

				def tmpServicePlan = tmpServicePlanMap[servicePlan.code]
				// need the id from the local plan and the price from the temp api plan
				servicePlan.id = tmpServicePlan.id
				priceSetPlans[priceSetCode] = servicePlan

				AccountPriceSet priceSet = new AccountPriceSet(
					name: priceSetName,
					code: priceSetCode,
					priceUnit: priceUnit,
					type: AccountPriceSet.PRICE_SET_TYPE.fixed.toString(),
					systemCreated: true
				)
				apiPriceSets << priceSet

				AccountPrice price = new AccountPrice(
					name: priceSetName,
					code: priceSetCode,
					active:true,
					priceType: AccountPrice.PRICE_TYPE.fixed,
					incurCharges: 'always',
					systemCreated: true,
					cost: new BigDecimal((servicePlan.getAt("price_${priceUnit}ly") ?: '0.0').toString()),
					priceUnit: priceUnit
				)
				apiPrices << price
				priceSetPrices[priceSetCode] = price

			}
		}

		Observable<AccountPriceSetIdentityProjection> existingPriceSets = morpheusContext.accountPriceSet.listSyncProjectionsByCode(priceSetCodes)
		SyncTask<AccountPriceSetIdentityProjection, AccountPriceSet, AccountPriceSet> syncTask = new SyncTask(existingPriceSets, apiPriceSets)
		syncTask.addMatchFunction { AccountPriceSetIdentityProjection projection, AccountPriceSet apiItem ->
			return projection.code.toString() == apiItem.code.toString()
		}.onDelete { List<AccountPriceSetIdentityProjection> deleteList ->
			def deleteIds = deleteList.collect { it.id }
			List<ServicePlanPriceSet> servicePlanPriceSetDeleteList = morpheusContext.servicePlanPriceSet.listByAccountPriceSetIds(deleteIds).toList().blockingGet()
			Boolean servicePlanPriceSetDeleteResult = morpheusContext.servicePlanPriceSet.remove(servicePlanPriceSetDeleteList).blockingGet()
			if(servicePlanPriceSetDeleteResult) {
				morpheusContext.accountPriceSet.remove(deleteList).blockingGet()
			} else {
				log.error("Failed to delete ServicePlanPriceSets associated to AccountPriceSet")
			}
		}.onAdd { List<AccountPriceSet> createList ->
			while (createList.size() > 0) {
				List chunkedList = createList.take(50)
				createList = createList.drop(50)
				createPriceSets(chunkedList, priceSetPlans)
			}
		}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet>> updateItems ->
			Map<Long, SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
			morpheusContext.accountPriceSet.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map {AccountPriceSet priceSet ->
				SyncTask.UpdateItemDto<AccountPriceSetIdentityProjection, AccountPriceSet> matchItem = updateItemMap[priceSet.id]
				return new SyncTask.UpdateItem<AccountPriceSet,AccountPriceSet>(existingItem:priceSet, masterItem:matchItem.masterItem)
			}
		}.onUpdate { updateList ->
			while (updateList.size() > 0) {
				List chunkedList = updateList.take(50)
				updateList = updateList.drop(50)
				updateMatchedPriceSet(chunkedList, priceSetPlans)
			}
		}.observe().blockingSubscribe() { complete ->
			if(complete) {
				Observable<AccountPriceIdentityProjection> existingPrices = morpheusContext.accountPrice.listSyncProjectionsByCode(priceSetCodes)
				SyncTask<AccountPriceIdentityProjection, AccountPrice, AccountPrice> priceSyncTask = new SyncTask(existingPrices, apiPrices)
				priceSyncTask.addMatchFunction { AccountPriceIdentityProjection projection, AccountPrice apiItem ->
					projection.code == apiItem.code
				}.onDelete { List<AccountPriceIdentityProjection> deleteList ->
					morpheusContext.accountPrice.remove(deleteList).blockingGet()
				}.onAdd { createList ->
					while(createList.size() > 0) {
						List chunkedList = createList.take(50)
						createList = createList.drop(50)
						createPrice(chunkedList)
					}
				}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice>> updateItems ->
					Map<Long, SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
					morpheusContext.accountPrice.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map { AccountPrice price ->
						SyncTask.UpdateItemDto<AccountPriceIdentityProjection, AccountPrice> matchItem = updateItemMap[price.id]
						return new SyncTask.UpdateItem<AccountPrice, AccountPrice>(existingItem: price, masterItem: matchItem.masterItem)
					}
				}.onUpdate { updateList ->
					while (updateList.size() > 0) {
						List chunkedList = updateList.take(50)
						updateList = updateList.drop(50)
						updateMatchedPrice(chunkedList)
					}
				}.start()
			}
		}

	}

	def createPriceSets(List<AccountPriceSet> createList, Map<String, ServicePlan> priceSetPlans) {
		log.debug("createPriceSets: createList count: ${createList.size()}, priceSetPlans count: ${priceSetPlans.size}")
		Boolean priceSetsCreated = morpheusContext.accountPriceSet.create(createList).blockingGet()
		if(priceSetsCreated) {
			syncServicePlanPriceSets(createList, priceSetPlans)
		}
	}

	def updateMatchedPriceSet(List<SyncTask.UpdateItem<AccountPriceSet,AccountPriceSet>> updateItems, Map<String, ServicePlan> priceSetPlans) {
		List<AccountPriceSet> itemsToUpdate = []
		updateItems.each {it ->
			AccountPriceSet remoteItem = it.masterItem
			AccountPriceSet localItem = it.existingItem
			def save = false

			if(localItem.name != remoteItem.name) {
				localItem.name = remoteItem.name
				save = true
			}

			if(save) {
				itemsToUpdate << localItem
			}
		}

		if(itemsToUpdate.size() > 0) {
			morpheusContext.accountPriceSet.save(itemsToUpdate).blockingGet()
		}

		syncServicePlanPriceSets(updateItems.collect { it.existingItem }, priceSetPlans)
	}

	def syncServicePlanPriceSets(List<AccountPriceSet> priceSets, Map<String, ServicePlan> priceSetPlans) {
		log.debug("syncServicePlanPriceSets: priceSets count: ${priceSets.size()}, servicePlans count: ${priceSetPlans.size()}")
		List<ServicePlanPriceSet> apiItems = priceSets?.collect { AccountPriceSet priceSet ->
			new ServicePlanPriceSet(priceSet: priceSet, servicePlan: priceSetPlans[priceSet.code])
		}

		Observable<ServicePlanPriceSetIdentityProjection> existingItems = morpheusContext.servicePlanPriceSet.listSyncProjections(priceSets)
		SyncTask<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet, ServicePlanPriceSet> syncTask = new SyncTask(existingItems, apiItems)
		syncTask.addMatchFunction { ServicePlanPriceSetIdentityProjection projection, ServicePlanPriceSet apiItem ->
			projection.priceSet.code == apiItem.priceSet.code && projection.servicePlan.code == apiItem.servicePlan.code
		}.onDelete { List<ServicePlanPriceSetIdentityProjection> deleteList ->
			morpheusContext.servicePlanPriceSet.remove(deleteList).blockingGet()
		}.onAdd { createList ->
			while(createList.size() > 0) {
				List chunkedList = createList.take(50)
				createList = createList.drop(50)
				log.debug("ServicePlanPriceSet createList: $chunkedList")
				morpheusContext.servicePlanPriceSet.create(chunkedList).blockingGet()
			}
		}.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet>> updateItems ->
			Map<Long, SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
			morpheusContext.servicePlanPriceSet.listById(updateItems.collect { it.existingItem.id } as Collection<Long>).map { ServicePlanPriceSet servicePlanPriceSet ->
				SyncTask.UpdateItemDto<ServicePlanPriceSetIdentityProjection, ServicePlanPriceSet> matchItem = updateItemMap[servicePlanPriceSet.id]
				return new SyncTask.UpdateItem<ServicePlanPriceSet, ServicePlanPriceSet>(existingItem: servicePlanPriceSet, masterItem: matchItem.masterItem)
			}
		}.onUpdate { updateList ->
			log.debug("ServicePlanPriceSet updateList: $updateList")
			// do nothing
		}.start()
	}

	def createPrice(List<AccountPrice> createList) {
		Boolean itemsCreated = morpheusContext.accountPrice.create(createList).blockingGet()
		if(itemsCreated) {
			List<String> priceSetCodes = createList.collect { it.code }
			Map<String, AccountPriceSet> tmpPriceSets = morpheusContext.accountPriceSet.listByCode(priceSetCodes).toList().blockingGet().collectEntries { [(it.code): it] }
			morpheusContext.accountPrice.listByCode(priceSetCodes).blockingSubscribe { AccountPrice price ->
				AccountPriceSet priceSet = tmpPriceSets[price.code]
				if(priceSet) {
					morpheusContext.accountPriceSet.addToPriceSet(priceSet, price).blockingGet()
				} else {
					log.error("createPrice addToPriceSet: Could not find matching price set for code {}", price.code)
				}
			}
		}
	}

	def updateMatchedPrice(List<SyncTask.UpdateItem<AccountPrice,AccountPrice>> updateItems) {
		log.debug("updateMatchedPrice updateItems: ${updateItems.size()}")
		// update price for pricing changes
		List<AccountPrice> itemsToUpdate = []
		updateItems.each {it ->
			AccountPrice remoteItem = it.masterItem
			AccountPrice localItem = it.existingItem
			def save = false

			if(localItem.cost != remoteItem.cost) {
				localItem.cost = remoteItem.cost
				save = true
			}

			if(save) {
				itemsToUpdate << localItem
			}
		}

		if(itemsToUpdate.size() > 0) {
			Boolean itemsUpdated = morpheusContext.accountPrice.save(itemsToUpdate).blockingGet()
			if(itemsUpdated) {
				List<String> priceSetCodes = itemsToUpdate.collect { it.code }
				Map<String, AccountPriceSet> tmpPriceSets = morpheusContext.accountPriceSet.listByCode(priceSetCodes).toList().blockingGet().collectEntries { [(it.code): it] }
				morpheusContext.accountPrice.listByCode(priceSetCodes).blockingSubscribe { AccountPrice price ->
					AccountPriceSet priceSet = tmpPriceSets[price.code]
					if(priceSet) {
						morpheusContext.accountPriceSet.addToPriceSet(priceSet, price).blockingGet()
					} else {
						log.error("createPrice addToPriceSet: Could not find matching price set for code {}", price.code)
					}
				}
			}
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

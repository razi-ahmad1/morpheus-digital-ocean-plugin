package com.morpheusdata.digitalocean

import com.morpheusdata.digitalocean.cloud.DigitalOceanCloudProvider
import com.morpheusdata.digitalocean.backup.DigitalOceanBackupProvider
import com.morpheusdata.digitalocean.DigitalOceanOptionSourceProvider
import com.morpheusdata.digitalocean.provisioning.DigitalOceanProvisionProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.AccountCredential
import com.morpheusdata.model.Cloud
import groovy.util.logging.Slf4j

@Slf4j
class DigitalOceanPlugin extends Plugin {

	@Override
	String getCode() {
		return 'morpheus-digital-ocean-plugin'
	}

	@Override
	void initialize() {
		this.name = 'DigitalOcean Plugin'

		// shared instance of the api service for the all providers to ensure the
		// api throttle rate is shared
		def apiService = new DigitalOceanApiService()

		DigitalOceanCloudProvider cloudProvider = new DigitalOceanCloudProvider(this, morpheus, apiService)
		DigitalOceanProvisionProvider provisionProvider = new DigitalOceanProvisionProvider(this, morpheus, apiService)
		DigitalOceanOptionSourceProvider optionSourceProvider = new DigitalOceanOptionSourceProvider(this, morpheus)
		pluginProviders.put(provisionProvider.code, provisionProvider)
		pluginProviders.put(cloudProvider.code, cloudProvider)
		pluginProviders.put(optionSourceProvider.code, optionSourceProvider)

		DigitalOceanBackupProvider backupProvider = new DigitalOceanBackupProvider(this, morpheus)
		pluginProviders.put(backupProvider.code, backupProvider)
	}

	@Override
	void onDestroy() {
	}

	MorpheusContext getMorpheusContext() {
		return morpheus
	}

	Map getAuthConfig(Cloud cloud) {
		log.debug "getAuthConfig: ${cloud}"
		def rtn = [:]

		if(!cloud.accountCredentialLoaded) {
			AccountCredential accountCredential
			try {
				accountCredential = this.morpheus.cloud.loadCredentials(cloud.id).blockingGet()
			} catch(e) {
				// If there is no credential on the cloud, then this will error
			}
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}

		log.debug("AccountCredential loaded: $cloud.accountCredentialLoaded, Data: $cloud.accountCredentialData")

		def username
		if(cloud.accountCredentialData && cloud.accountCredentialData.containsKey('username')) {
			username = cloud.accountCredentialData['username']
		} else {
			username = cloud.configMap.username
		}
		def apiKey
		if(cloud.accountCredentialData && cloud.accountCredentialData.containsKey('password')) {
			apiKey = cloud.accountCredentialData['password']
		} else {
			apiKey = cloud.configMap.apiKey
		}

		rtn.doUsername = username
		rtn.doApiKey = apiKey
		return rtn
	}
}

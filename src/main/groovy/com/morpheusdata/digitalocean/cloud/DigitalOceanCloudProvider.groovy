package com.morpheusdata.digitalocean.cloud

import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.digitalocean.cloud.sync.DatacentersSync
import com.morpheusdata.digitalocean.cloud.sync.ImagesSync
import com.morpheusdata.digitalocean.cloud.sync.SizesSync
import com.morpheusdata.core.backup.BackupProvider;
import com.morpheusdata.core.CloudProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.ProvisioningProvider
import com.morpheusdata.model.*
import com.morpheusdata.request.ValidateCloudRequest
import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity

@Slf4j
class DigitalOceanCloudProvider implements CloudProvider {
	DigitalOceanPlugin plugin
	MorpheusContext morpheusContext
	DigitalOceanApiService apiService

	public static String LINUX_VIRTUAL_IMAGE_CODE = 'digitalOceanLinux'

	DigitalOceanCloudProvider(DigitalOceanPlugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
		apiService = new DigitalOceanApiService()
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
	Icon getIcon() {
		return new Icon(path:"digitalocean.svg", darkPath: "digitalocean.svg")
	}

	@Override
	Icon getCircularIcon() {
		return new Icon(path:"digitalocean-circle.svg", darkPath: "digitalocean-circle.svg")
	}

	@Override
	String getCode() {
		return 'digitalocean'
	}

	@Override
	String getName() {
		return 'DigitalOcean'
	}

	@Override
	String getDescription() {
		return 'DigitalOcean'
	}

	@Override
	String getDefaultProvisionTypeCode() {
		return 'digitalocean-provision-provider'
	}

	@Override
	Boolean hasComputeZonePools() {
		return false
	}

	@Override
	Boolean hasNetworks() {
		return false
	}

	@Override
	Boolean hasFolders() {
		return false
	}

	@Override
	Boolean hasCloudInit() {
		false
	}

	@Override
	Boolean hasDatastores() {
		return false
	}

	@Override
	Boolean hasBareMetal() {
		return false
	}

	@Override
	Boolean supportsDistributedWorker() {
		false
	}

	@Override
	Collection<OptionType> getOptionTypes() {
		def options = []
		options << new OptionType(
				code: 'zoneType.digitalocean.credential',
				inputType: OptionType.InputType.CREDENTIAL,
				name: 'Credentials',
				fieldName: 'type',
				fieldLabel: 'Credentials',
				fieldContext: 'credential',
				required: true,
				defaultValue: 'local',
				displayOrder: 0,
				optionSource: 'credentials',
				config: '{"credentialTypes":["username-api-key"]}'
		)
		options << new OptionType(
				name: 'Username',
				code: 'zoneType.digitalocean.username',
				fieldName: 'username',
				displayOrder: 10,
				fieldCode: 'gomorpheus.optiontype.Username',
				fieldLabel: 'Username',
				required: true,
				inputType: OptionType.InputType.TEXT,
				fieldContext: 'config',
				localCredential: true
		)
		options << new OptionType(
				name: 'API Key',
				code: 'zoneType.digitalocean.apiKey',
				fieldName: 'apiKey',
				displayOrder: 20,
				fieldCode: 'gomorpheus.optiontype.ApiKey',
				fieldLabel: 'API Key',
				required: true,
				inputType: OptionType.InputType.PASSWORD,
				fieldContext: 'config',
				localCredential: true
		)
		options << new OptionType(
				name: 'Datacenter',
				code: 'zoneType.digitalocean.datacenter',
				fieldName: 'datacenter',
				optionSource: 'digitalOceanDataCenters',
				displayOrder: 30,
				fieldCode: 'gomorpheus.optiontype.Datacenter',
				fieldLabel: 'Datacenter',
				required: true,
				inputType: OptionType.InputType.SELECT,
				dependsOn: 'config.apiKey, apiKey, credential',
				fieldContext: 'config'
		)
		return options
	}

	@Override
	Collection<ComputeServerType> getComputeServerTypes() {
		//digital ocean
		def serverTypes = [
				new ComputeServerType(code: 'digitalOceanWindows', name: 'DigitalOcean Windows Node', description: '', platform: PlatformType.windows, agentType: ComputeServerType.AgentType.host,
						enabled: true, selectable: false, externalDelete: true, managed: true, controlPower: true, controlSuspend: false, creatable: false, computeService: null,
						displayOrder: 17, hasAutomation: true, reconfigureSupported: true, provisionTypeCode: 'digitalocean',
						containerHypervisor: true, bareMetalHost: false, vmHypervisor: false, guestVm: true,
						optionTypes: []
				),

				new ComputeServerType(code: 'digitalOceanVm', name: 'DigitalOcean VM Instance', description: '', platform: PlatformType.linux,
						enabled: true, selectable: false, externalDelete: true, managed: true, controlPower: true, controlSuspend: false, creatable: false, computeService: null,
						displayOrder: 0, hasAutomation: true, reconfigureSupported: true, provisionTypeCode: 'digitalocean',
						containerHypervisor: false, bareMetalHost: false, vmHypervisor: false, agentType: ComputeServerType.AgentType.guest, guestVm: true,
						optionTypes: []
				),

				//docker
				new ComputeServerType(code: LINUX_VIRTUAL_IMAGE_CODE, name: 'DigitalOcean Docker Host', description: '', platform: PlatformType.linux,
						enabled: true, selectable: false, externalDelete: true, managed: true, controlPower: true, controlSuspend: false, creatable: true, computeService: null,
						displayOrder: 16, hasAutomation: true, reconfigureSupported: true, provisionTypeCode: 'digitalocean',
						containerHypervisor: true, bareMetalHost: false, vmHypervisor: false, agentType: ComputeServerType.AgentType.host, clusterType: ComputeServerType.ClusterType.docker,
						computeTypeCode: 'docker-host',
						optionTypes: []
				),

				//kubernetes
				new ComputeServerType(code: 'digitalOceanKubeMaster', name: 'Digital Ocean Kubernetes Master', description: '', platform: PlatformType.linux,
						reconfigureSupported: true, enabled: true, selectable: false, externalDelete: true, managed: true, controlPower: true, controlSuspend: true, creatable: true,
						supportsConsoleKeymap: true, computeService: null, displayOrder: 10,
						hasAutomation: true, containerHypervisor: true, bareMetalHost: false, vmHypervisor: false, agentType: ComputeServerType.AgentType.host, clusterType: ComputeServerType.ClusterType.kubernetes,
						computeTypeCode: 'kube-master',
						optionTypes: []
				),
				new ComputeServerType(code: 'digitalOceanKubeWorker', name: 'Digital Ocean Kubernetes Worker', description: '', platform: PlatformType.linux,
						reconfigureSupported: true, enabled: true, selectable: false, externalDelete: true, managed: true, controlPower: true, controlSuspend: true, creatable: true,
						supportsConsoleKeymap: true, computeService: null, displayOrder: 10,
						hasAutomation: true, containerHypervisor: true, bareMetalHost: false, vmHypervisor: false, agentType: ComputeServerType.AgentType.host, clusterType: ComputeServerType.ClusterType.kubernetes,
						computeTypeCode: 'kube-worker',
						optionTypes: []
				),
				//unmanaged discovered type
				new ComputeServerType(code: 'digitalOceanUnmanaged', name: 'Digital Ocean VM', description: 'Digital Ocean VM', platform: PlatformType.none, agentType: ComputeServerType.AgentType.guest,
						enabled: true, selectable: false, externalDelete: true, managed: false, controlPower: true, controlSuspend: false, creatable: false, computeService: null,
						displayOrder: 99, hasAutomation: false, provisionTypeCode: 'digitalocean',
						containerHypervisor: false, bareMetalHost: false, vmHypervisor: false, managedServerType: 'digitalOceanVm2', guestVm: true, supportsConsoleKeymap: true,
						optionTypes: []
				)
		]

		return serverTypes
	}

	@Override
	Collection<ProvisioningProvider> getAvailableProvisioningProviders() {
		return plugin.getProvidersByType(ProvisioningProvider) as Collection<ProvisioningProvider>
	}

	@Override
	Collection<BackupProvider> getAvailableBackupProviders() {
		return plugin.getProvidersByType(BackupProvider) as Collection<com.morpheusdata.core.backup.BackupProvider>
	}

	@Override
	ProvisioningProvider getProvisioningProvider(String providerCode) {
		return getAvailableProvisioningProviders().find { it.code == providerCode }
	}

	@Override
	Collection<NetworkType> getNetworkTypes() {
		return null
	}

	@Override
	Collection<NetworkSubnetType> getSubnetTypes() {
		return null
	}

	@Override
	Collection<StorageVolumeType> getStorageVolumeTypes() {
		return null
	}

	@Override
	Collection<StorageControllerType> getStorageControllerTypes() {
		return null
	}

	@Override
	ServiceResponse validate(Cloud zoneInfo, ValidateCloudRequest validateCloudRequest) {
		log.debug("validating Cloud: ${zoneInfo.code}, ${validateCloudRequest.credentialType} ${validateCloudRequest.credentialUsername} ${validateCloudRequest.credentialPassword}")
		if (!zoneInfo.configMap.datacenter) {
			return new ServiceResponse(success: false, msg: 'Choose a datacenter')
		}
		def apiKey
		def username

		if(validateCloudRequest.credentialType == 'local') {
			apiKey = zoneInfo.configMap.doApiKey
			username = zoneInfo.configMap.doUsername
		} else if (validateCloudRequest.credentialType && validateCloudRequest.credentialType.isNumber()) {
			def credentialId = validateCloudRequest.credentialType.toLong()
			AccountCredential accountCredential = morpheusContext.accountCredential.get(credentialId).blockingGet()
			apiKey = accountCredential.data?.password
			username = accountCredential.data?.username
		} else {
			apiKey = validateCloudRequest.credentialPassword
			username = validateCloudRequest.credentialUsername
		}

		if (!username) {
			return new ServiceResponse(success: false, msg: 'Enter a username')
		}
		if (!apiKey) {
			return new ServiceResponse(success: false, msg: 'Enter your api key')
		}

		ServiceResponse response = apiService.listRegions(apiKey)
		if(response.success == false) {
			return new ServiceResponse(success: false, msg: 'Invalid credentials')
		}

		return new ServiceResponse(success: true)
	}

	@Override
	ServiceResponse initializeCloud(Cloud cloud) {
		ServiceResponse serviceResponse
		log.debug("Initializing Cloud: ${cloud.code}")
		log.debug("config: ${cloud.configMap}")
		String apiKey = plugin.getAuthConfig(cloud).doApiKey

		// check account
		ServiceResponse initResponse = apiService.getAccount(apiKey)
		if (initResponse.success && initResponse.data.status == 'active') {
			serviceResponse = new ServiceResponse(success: true, content: initResponse.content)

			refreshDaily(cloud)
			refresh(cloud)

			KeyPair keyPair = morpheusContext.cloud.findOrGenerateKeyPair(cloud.account).blockingGet()
			if (keyPair) {
				KeyPair updatedKeyPair = findOrUploadKeypair(apiKey, keyPair.publicKey, keyPair.name)
				if(updatedKeyPair) {
					morpheusContext.cloud.updateKeyPair(updatedKeyPair, cloud)
				} else {
					log.warn("Unable to create SSH Key pair")
				}
			} else {
				log.debug("no morpheus keys found")
			}
		} else {
			serviceResponse = new ServiceResponse(success: false, msg: respMap.resp?.statusLine?.statusCode, content: respMap.json)
		}

		serviceResponse
	}

	@Override
	ServiceResponse refresh(Cloud cloud) {
		log.debug("Short refresh cloud ${cloud.code}")
		(new ImagesSync(plugin, cloud, apiService)).execute()
		log.debug("Completed short refresh for cloud $cloud.code")
		return ServiceResponse.success()
	}

	@Override
	void refreshDaily(Cloud cloud) {
		log.debug("daily refresh cloud ${cloud.code}")
		(new SizesSync(plugin, cloud, apiService)).execute()
		log.debug("Completed daily refresh for cloud ${cloud.code}")
	}

	@Override
	ServiceResponse deleteCloud(Cloud cloudInfo) {
		return new ServiceResponse(success: true)
	}

	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		String dropletId = computeServer.externalId
		String apiKey = plugin.getAuthConfig(computeServer.cloud).doApiKey
		log.debug("startServer: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}
		def body = ['type': 'power_on']
		apiService.performDropletAction(apiKey, dropletId, body)
	}

	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		String dropletId = computeServer.externalId
		String apiKey = plugin.getAuthConfig(computeServer.cloud).doApiKey
		log.debug("stopServer: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}
		def body = ['type': 'shutdown']
		apiService.performDropletAction(apiKey, dropletId, body)
	}

	@Override
	ServiceResponse deleteServer(ComputeServer computeServer) {
		String dropletId = computeServer.externalId
		String apiKey = plugin.getAuthConfig(computeServer.cloud).doApiKey
		log.debug("deleteServer: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}
		ServiceResponse response = apiService.deleteDroplet(apiKey, dropletId)
		if (response.success) {
			return new ServiceResponse(success: true)
		} else {
			return new ServiceResponse(success: false, results: response.results, data: response.data, msg: response.errorCode, error: response.results)
		}
	}

	KeyPair findOrUploadKeypair(String apiKey, String publicKey, String keyName) {
		KeyPair rtn = null
		Map match = null
		keyName = keyName ?: 'morpheus_do_plugin_key'
		log.debug("find or update keypair for key $keyName")
		ServiceResponse keysResponse = apiService.listAccountKeys(apiKey)
		if(keysResponse.success) {
			match = keysResponse.data.find { publicKey.startsWith(it['public_key']) }
			log.debug("match: ${match} - list: ${keysResponse.data}")
			if (!match) {
				log.debug('key not found in DO')
				ServiceResponse response = apiService.createAccountKey(apiKey, keyName, publicKey)
				if (response.success) {
					match = response.data
				} else {
					log.debug('failed to add DO ssh key')
				}
			}
		}

		if(match) {
			rtn = new KeyPair(name: match.name, externalId: match.id, publicKey: match.public_key, publicFingerprint: match.fingerprint)
		}

		return rtn
	}
}

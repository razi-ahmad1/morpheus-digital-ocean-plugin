package com.morpheusdata.digitalocean.provisioning

import com.morpheusdata.digitalocean.DigitalOceanPlugin
import com.morpheusdata.digitalocean.DigitalOceanApiService
import com.morpheusdata.core.AbstractProvisionProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerInterfaceType
import com.morpheusdata.model.ComputeTypeLayout
import com.morpheusdata.model.ComputeTypeSet
import com.morpheusdata.model.ContainerType
import com.morpheusdata.model.HostType
import com.morpheusdata.model.ImageType
import com.morpheusdata.model.Instance
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.OsType
import com.morpheusdata.model.ServicePlan
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.model.Workload
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.KeyPair
import com.morpheusdata.model.provisioning.HostRequest
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.model.provisioning.UsersConfiguration
import com.morpheusdata.request.ResizeRequest
import com.morpheusdata.response.HostResponse
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.response.WorkloadResponse
import groovy.util.logging.Slf4j

@Slf4j
class DigitalOceanProvisionProvider extends AbstractProvisionProvider {
	DigitalOceanPlugin plugin
	MorpheusContext morpheusContext
	private static final String DIGITAL_OCEAN_ENDPOINT = 'https://api.digitalocean.com'
	private static final String UBUNTU_VIRTUAL_IMAGE_CODE = 'digitalocean.image.morpheus.ubuntu.18.04'
	DigitalOceanApiService apiService

	DigitalOceanProvisionProvider(DigitalOceanPlugin plugin, MorpheusContext morpheusContext, DigitalOceanApiService apiService) {
		this.plugin = plugin
		this.morpheusContext = morpheusContext
		this.apiService = apiService ?: new DigitalOceanApiService()
	}

	@Override
	ServiceResponse createWorkloadResources(Workload workload, Map opts) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	@Override
	HostType getHostType() {
		HostType.vm
	}

	@Override
	Collection<VirtualImage> getVirtualImages() {
		VirtualImage virtualImage = new VirtualImage(
				code: UBUNTU_VIRTUAL_IMAGE_CODE,
				category:'digitalocean.image.morpheus',
				name:'Ubuntu 18.04 LTS (Digital Ocean Marketplace)',
				imageType: ImageType.qcow2,
				systemImage:true,
				isCloudInit:true,
				externalId:'ubuntu-18-04-x64',
				osType: new OsType(code: 'ubuntu.18.04.64')
		)
		[virtualImage]
	}

	@Override
	Collection<ComputeTypeLayout> getComputeTypeLayouts() {
		List<ComputeTypeLayout> layouts = []
		layouts << this.morpheusContext.getComputeTypeLayoutFactoryService().buildDockerLayout(
				'docker-digitalOcean-ubuntu-18.04',
				'18.04',
				this.provisionTypeCode,
			'digitalOceanLinux',
				UBUNTU_VIRTUAL_IMAGE_CODE
		).blockingGet()

		return layouts
	}

	@Override
	public Collection<OptionType> getNodeOptionTypes() {
		def options = [
			new OptionType(
				name: 'image',
				category: "provisionType.digitalOcean.custom",
				code: 'provisionType.digitalOcean.custom.containerType.imageId',
				fieldContext: 'domain',
				fieldName: 'virtualImage.id',
				fieldCode: 'gomorpheus.label.vmImage',
				fieldLabel: 'VM Image',
				fieldGroup: null,
				inputType: OptionType.InputType.SELECT,
				displayOrder: 10,
				required: true,
				optionSourceType: 'digitalOcean',
				optionSource: 'digitalOceanImage'
			),
			new OptionType(
				name: 'mount logs',
				category: "provisionType.digitalOcean.custom",
				code: 'provisionType.digitalOcean.custom.containerType.mountLogs',
				fieldContext: 'domain',
				fieldName: 'mountLogs',
				fieldCode: 'gomorpheus.optiontype.LogFolder',
				fieldLabel: 'Log Folder',
				fieldGroup: null,
				inputType: OptionType.InputType.TEXT,
				displayOrder: 20,
				required: false,
				editable: false
			),
			new OptionType(
				name: 'mount config',
				category: "provisionType.digitalOcean.custom",
				code: 'provisionType.digitalOcean.custom.containerType.mountConfig',
				fieldContext: 'domain',
				fieldName: 'mountConfig',
				fieldCode: 'gomorpheus.optiontype.ConfigFolder',
				fieldLabel: 'Config Folder',
				fieldGroup: null,
				inputType: OptionType.InputType.TEXT,
				displayOrder: 30,
				required: false,
				editable: false
			),
			new OptionType(
				name: 'mount data',
				category: "provisionType.digitalOcean.custom",
				code: 'provisionType.digitalOcean.custom.containerType.mountData',
				fieldContext: 'domain',
				fieldName: 'mountData',
				fieldCode: 'gomorpheus.optiontype.DeployFolder',
				fieldLabel: 'Deploy Folder',
				fieldGroup: null,
				inputType: OptionType.InputType.TEXT,
				displayOrder: 40,
				required: false,
				editable: false,
				helpTextI18nCode: "gomorpheus.help.deployFolder"
			)

		]

		return options
	}

	@Override
	Collection<OptionType> getOptionTypes() {
		def options = []
		options << new OptionType(
			name: 'skip agent install',
			code: 'provisionType.digitalOcean.noAgent',
			category: 'provisionType.digitalOcean',
			inputType: OptionType.InputType.CHECKBOX,
			fieldName: 'noAgent',
			fieldContext: 'config',
			fieldCode: 'gomorpheus.optiontype.SkipAgentInstall',
			fieldLabel: 'Skip Agent Install',
			fieldGroup:'Advanced Options',
			displayOrder: 4,
			required: false,
			enabled: true,
			editable:false,
			global:false,
			placeHolder:null,
			helpBlock:'Skipping Agent installation will result in a lack of logging and guest operating system statistics. Automation scripts may also be adversely affected.',
			defaultValue:null,
			custom:false,
			fieldClass:null
		)

		return options
	}

	@Override
	Collection<ServicePlan> getServicePlans() {
		return []
	}

	@Override
	Collection<ComputeServerInterfaceType> getComputeServerInterfaceTypes() {
		return []
	}

	@Override
	String getCode() {
		return 'digitalocean-provision-provider'
	}

	@Override
	String getProvisionTypeCode() {
		return "digitalocean"
	}

	@Override
	String getName() {
		return 'Digital Ocean'
	}

	@Override
	Boolean hasDatastores() {
		return false
	}

	@Override
	Boolean hasNetworks() {
		return false
	}

	@Override
	Boolean hasPlanTagMatch() {
		false
	}

	@Override
	Integer getMaxNetworks() {
		return 1
	}

	@Override
	Boolean supportsCustomServicePlans() {
		return false
	}

	@Override
	Boolean hasNodeTypes() {
		return true
	}

	@Override
	Boolean customSupported() {
		return true
	}

	@Override
	Boolean lvmSupported() {
		return false
	}

	@Override
	Boolean createServer() {
		return true
	}

	@Override
	ServiceResponse validateWorkload(Map opts) {
		log.debug("validateWorkload: ${opts}")
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse validateInstance(Instance instance, Map opts) {
		log.debug("validateInstance, instance: ${instance}, opts: ${opts}")
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse validateDockerHost(ComputeServer server, Map opts) {
		log.debug("validateDockerHost, server: ${server}, opts: ${opts}")
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse prepareWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		log.debug("prepareWorkload, workload: ${workload}, workloadRequest: ${workloadRequest}, opts: ${opts}")
		return ServiceResponse.success()
	}

	@Override
	ServiceResponse<WorkloadResponse> runWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		log.debug("runWorkload: ${workload.configs} ${opts}")
		def containerConfig = new groovy.json.JsonSlurper().parseText(workload.configs ?: '{}')
		ComputeServer server = workload.server
		Cloud cloud = server?.cloud

		String apiKey = plugin.getAuthConfig(cloud).doApiKey
		if (!apiKey) {
			return new ServiceResponse(success: false, msg: 'No API Key provided')
		}
		Map callbackOpts = [:]

		def imageId = opts.imageRef
		log.debug("runWorkload: cloneContainerId: $opts.cloneContainerId, opts.backupSetId: $opts.backupSetId")
		if(opts.cloneContainerId && opts.backupSetId) {
			BackupResult backupResult = plugin.morpheus.backup.backupResult.listByBackupSetIdAndContainerId(opts.backupSetId, opts.cloneContainerId).blockingFirst()
			log.debug("runWorkload backupResults: $backupResult")
			if(backupResult) {
				def snapshotImageId = backupResult.externalId
				if(snapshotImageId){
					log.debug("creating server from snapshot image: ${snapshotImageId}")
					imageId = snapshotImageId
				}
			}
		}

		VirtualImage virtualImage = workload.server.sourceImage
		// Grab the user configuration data (then update the server)
		UsersConfiguration usersConfiguration = morpheus.provision.getUserConfig(workload, virtualImage, opts).blockingGet()
		log.debug("usersConfiguration ${usersConfiguration}")
		server.sshUsername = usersConfiguration.sshUsername
		server.sshPassword = usersConfiguration.sshPassword
		morpheus.computeServer.save([server]).blockingGet()

		def userData
		if(virtualImage?.isCloudInit) {
			// Utilize the morpheus build cloud-init methods
			Map cloudConfigOptions = workloadRequest.cloudConfigOpts
			log.debug("cloudConfigOptions ${cloudConfigOptions}")

			// Inform Morpheus to install the agent (or not) after the server is created
			callbackOpts.noAgent = (opts.config?.containsKey("noAgent") == true && opts.config.noAgent == true)
			callbackOpts.installAgent = (opts.config?.containsKey("noAgent") == false || (opts.config?.containsKey("noAgent") && opts.config.noAgent != true))

			def cloudConfigUser = workloadRequest.cloudConfigUser
			log.debug("cloudConfigUser: ${cloudConfigUser}")
			userData = cloudConfigUser

			// Not really used in DO provisioning (example only)
			String metadata = workloadRequest.cloudConfigMeta
			log.debug("metadata: ${metadata}")

			//// Not really used in DO provisioning (example only)
			//String networkData = morpheus.provision.buildCloudNetworkData(com.morpheusdata.model.PlatformType.valueOf(server.osType), cloudConfigOptions).blockingGet()
			//log.debug("networkData: ${networkData}")
		} else {
			// These users will be created by Morpheus after provisioning
			callbackOpts.createUsers = usersConfiguration.createUsers
		}

		// Now.. ready to create it in DO
		def dropletConfig = [
			'name'              : workload.server.getExternalHostname(),
			'region'            : cloud.configMap.datacenter,
			'size'              : workload.plan.externalId,
			'image'             : imageId,
			'backups'           : "${opts.doBackups == true}",
			'ipv6'              : "${opts.ipv6 == true}",
			'user_data'         : userData
		]

		// Add ssh keys provided by morpheus core services, e.g. Account or User ssh keys
		if(opts.sshKeys && opts.sshKeys.size() > 0) {
			dropletConfig.ssh_keys = opts.sshKeys.collect { it.toString() }
		}

		// add account ssh key if its not in the droplet config
		KeyPair accountKeyPair = morpheus.cloud.findOrGenerateKeyPair(workload.account).blockingGet()
		def keyPairId = accountKeyPair?.configMap["${cloud.code}.keyId"]
		if(keyPairId && (!dropletConfig.ssh_keys || dropletConfig.ssh_keys.contains(keyPairId.toString()) == false)) {
			dropletConfig.ssh_keys = dropletConfig.ssh_keys ?: []
			dropletConfig.ssh_keys << keyPairId.toString()
		}

		// log.debug("runWorkload dropletConfig: $dropletConfig")
		def response = apiService.createDroplet(apiKey, dropletConfig)
		if (response.success) {
			log.debug("Droplet Created ${response.results}")

			// Need to store the link between the Morpheus ComputeServer reference and the Digital Ocean object
			def droplet = response.data
			WorkloadResponse workloadResponse = dropletToWorkloadResponse(droplet, callbackOpts)

			def externalId = droplet.id
			server.externalId = externalId
			server.osDevice = '/dev/vda'
			server.dataDevice = '/dev/vda'
			server.lvmEnabled = false
			server = saveAndGet(server)

			return new ServiceResponse<WorkloadResponse>(success: true, data: workloadResponse)
		} else {
			log.debug("Failed to create droplet: $response.results")
			return new ServiceResponse(success: false, msg: response.errorCode, content: response.content, error: response.data)
		}
	}

	@Override
	ServiceResponse finalizeWorkload(Workload workload) {
		ServiceResponse rtn = ServiceResponse.prepare()
		log.debug("finalizeWorkload: ${workload?.id}")
		try {
			ComputeServer server = workload.server
			Cloud cloud = server.cloud
			def serverDetails = getServerDetails(server)

			// update IP address if necessary
			if(serverDetails.success == true && serverDetails.data.publicIp) {
				def doSave = false
				def privateIp = serverDetails.data.privateIp
				def publicIp = serverDetails.data.publicIp
				if(server.internalIp != privateIp) {
					server.internalIp = privateIp
					doSave = true
				}
				if(serverDetails.data.publicIp != publicIp) {
					server.externalIp = publicIp
					doSave = true
				}

				if(doSave) {
					morpheusContext.computeServer.save([server]).blockingGet()
				}
				rtn.success = true
			}
		} catch(e) {
			rtn.success = false
			rtn.msg = "Error in finalizing server: ${e.message}"
			log.error("Error in finalizeWorkload: {}", e, e)
		}

		return rtn
	}

	@Override
	ServiceResponse prepareHost(ComputeServer server, HostRequest hostRequest, Map opts) {
		log.debug("prepareHost: ${server} ${hostRequest} ${opts}")

		def rtn = [success: false, msg: null]
		try {
			VirtualImage virtualImage
			Long computeTypeSetId = server.typeSet?.id
			if(computeTypeSetId) {
				ComputeTypeSet computeTypeSet = morpheus.computeTypeSet.get(computeTypeSetId).blockingGet()
				if(computeTypeSet.containerType) {
					ContainerType containerType = morpheus.containerType.get(computeTypeSet.containerType.id).blockingGet()
					virtualImage = containerType.virtualImage
				}
			}
			if(!virtualImage) {
				rtn.msg = "No virtual image selected"
			} else {
				server.sourceImage = virtualImage
				saveAndGet(server)
				rtn.success = true
			}
		} catch(e) {
			rtn.msg = "Error in prepareHost: ${e}"
			log.error("${rtn.msg}, ${e}", e)

		}
		new ServiceResponse(rtn.success, rtn.msg, null, null)
	}

	@Override
	ServiceResponse<HostResponse> runHost(ComputeServer server, HostRequest hostRequest, Map opts) {
		log.debug("runHost: ${server} ${hostRequest} ${opts}")

		String apiKey = plugin.getAuthConfig(server.cloud).doApiKey
		if (!apiKey) {
			return new ServiceResponse(success: false, msg: 'No API Key provided')
		}
		
		// sshKeys needed?
//		opts.sshKeys = getKeyList(server.cloud, config.publicKeyId)

		def rtn = [success:false]
		def found = false
		log.debug("findOrCreateServer: ${server.externalId}")

		def dropletConfig = [
				'name'              : cleanInstanceName(server.name),
				'region'            : server.cloud.configMap.datacenter,
				'size'              : server.plan.externalId,
				'image'             : server.sourceImage.externalId,
				'backups'           : "${opts.doBackups}",
				'ipv6'              : false,
				'user_data'         : hostRequest.cloudConfigUser,
				'private_networking': false
		]

		def response = apiService.createDroplet(apiKey, dropletConfig)

		Map callbackOpts = [:]
		if(server.sourceImage?.isCloudInit) {
			// Utilize the morpheus built cloud-init methods
			Map cloudConfigOptions = hostRequest.cloudConfigOpts
			log.debug("cloudConfigOptions ${cloudConfigOptions}")

			// Inform Morpheus to install the agent (or not) after the server is created
			callbackOpts.installAgent = opts.installAgent && (cloudConfigOptions.installAgent != true)
		} else {
			// These users will be created by Morpheus after provisioning
			callbackOpts.createUsers = hostRequest.usersConfiguration
		}

		if (response.success) {
			log.debug("Droplet Created ${response.content}")

			def droplet = response.data
			HostResponse hostResponse = workloadResponseToHostResponse(dropletToWorkloadResponse(droplet, callbackOpts))

			// Need to store the link between the Morpheus ComputeServer reference and the Digital Ocean object
			server.externalId = hostResponse.externalId
			server.osDevice = '/dev/vda'
			server.dataDevice = '/dev/vda'
			server.lvmEnabled = false
			server = saveAndGet(server)

			return new ServiceResponse<HostResponse>(success: true, data: hostResponse)
		} else {
			log.debug("Failed to create droplet: $response.results")
			return new ServiceResponse(success: false, msg: response?.errorCode, content: response.content, error: response.data)
		}
	}

	@Override
	ServiceResponse<HostResponse> waitForHost(ComputeServer server) {
		log.debug("waitForHost: ${server}")
		try {
			ServiceResponse<WorkloadResponse> statusResults = getServerDetails(server)
			WorkloadResponse workloadResponse = statusResults.data
			HostResponse hostResponse = workloadResponseToHostResponse(workloadResponse)
			return new ServiceResponse<HostResponse>(statusResults.success, statusResults.msg, statusResults.errors, hostResponse)
		} catch(e) {
			log.error("Error waitForHost: ${e}", e)
			return new ServiceResponse(success: false, msg: "Error in waiting for Host: ${e}")
		}
	}

	@Override
	ServiceResponse finalizeHost(ComputeServer server) {
		ServiceResponse rtn = ServiceResponse.prepare()
		log.debug("finalizeHost: ${server?.id}")
		try {
			Cloud cloud = server.cloud
			def serverDetails = getServerDetails(server)

			// update IP address if necessary
			if(serverDetails.success == true && serverDetails.data.publicIp) {
				def doSave = false
				def privateIp = serverDetails.data.privateIp
				def publicIp = serverDetails.data.publicIp
				if(server.internalIp != privateIp) {
					server.internalIp = privateIp
					doSave = true
				}
				if(serverDetails.externalIp != publicIp) {
					server.externalIp = publicIp
					doSave = true
				}

				if(doSave) {
					morpheusContext.computeServer.save([server]).blockingGet()
				}
				rtn.success = true
			}
		} catch(e) {
			rtn.success = false
			rtn.msg = "Error in finalizing server: ${e.message}"
			log.error("Error in finalizeWorkload: {}", e, e)
		}

		return rtn
	}

	@Override
	ServiceResponse resizeServer(ComputeServer server, ResizeRequest resizeRequest, Map opts) {
		log.debug("resizeServer: ${server} ${resizeRequest} ${opts}")
		internalResizeServer(server, resizeRequest)
	}

	@Override
	ServiceResponse resizeWorkload(Instance instance, Workload workload, ResizeRequest resizeRequest, Map opts) {
		log.debug("resizeWorkload: ${instance} ${workload} ${resizeRequest} ${opts}")
		internalResizeServer(workload.server, resizeRequest)
	}

	private ServiceResponse internalResizeServer(ComputeServer server, ResizeRequest resizeRequest) {
		log.debug("internalResizeServer: ${server} ${resizeRequest}")
		ServiceResponse rtn = ServiceResponse.success()
		try {
			String apiKey = plugin.getAuthConfig(server.cloud).doApiKey
			String dropletId = server.externalId
			Map actionConfig = [
					'disk': true,
					'size': resizeRequest.plan.externalId
			]
			rtn = apiService.performDropletAction(apiKey, dropletId, 'resize', actionConfig)
			if(rtn.success) {
				StorageVolume existingVolume = server.volumes?.getAt(0)
				if (existingVolume) {
					existingVolume.maxStorage = resizeRequest.maxStorage
					morpheus.storageVolume.save([existingVolume]).blockingGet()
				}
			}
		} catch(e) {
			rtn.success = false
			rtn.msg = e.message
			log.error("Error in resizing server: ${e}", e)
		}
		rtn
	}

	@Override
	ServiceResponse<WorkloadResponse> stopWorkload(Workload workload) {
		String dropletId = workload.server.externalId
		String apiKey = plugin.getAuthConfig(workload.server.cloud).doApiKey
		log.debug("stopWorkload: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}

		ServiceResponse response = apiService.performDropletAction(apiKey, dropletId, 'shutdown')
		if (response.success) {
			return apiService.checkActionComplete(apiKey, response.data.id.toString())
		} else {
			powerOffServer(apiKey, dropletId)
		}
	}

	@Override
	ServiceResponse<WorkloadResponse> startWorkload(Workload workload) {
		String dropletId = workload.server.externalId
		String apiKey = plugin.getAuthConfig(workload.server.cloud).doApiKey
		log.debug("startWorkload: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}
		return apiService.performDropletAction(apiKey, dropletId, 'power_on')
	}

	@Override
	ServiceResponse restartWorkload(Workload workload) {
		ServiceResponse rtn = ServiceResponse.prepare()
		log.debug("restartWorkload: ${workload.id}")
		ServiceResponse stopResult = stopWorkload(workload)
		if (stopResult.success) {
			rtn = startWorkload(workload)
		} else {
			rtn = stopResult
		}

		return rtn
	}

	@Override
	ServiceResponse removeWorkload(Workload workload, Map opts) {
		String dropletId = workload.server.externalId
		log.debug("removeWorkload: ${dropletId}")
		if (!dropletId) {
			log.debug("no Droplet ID provided")
			return new ServiceResponse(success: false, msg: 'No Droplet ID provided')
		}

		String apiKey = plugin.getAuthConfig(workload.server.cloud).doApiKey
		return apiService.deleteDroplet(apiKey, dropletId)
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
	ServiceResponse<WorkloadResponse> getServerDetails(ComputeServer server) {
		log.debug("getServerDetails: $server.id")
		ServiceResponse rtn = ServiceResponse.prepare(new WorkloadResponse())
		String apiKey = plugin.getAuthConfig(server.cloud).doApiKey

		Boolean pending = true
		Integer attempts = 0
		while (pending) {
			log.debug("getServerDetails attempt $attempts")
			sleep(1000l * 20l)
			def resp = apiService.getDroplet(apiKey, server.externalId)
			if (resp.success) {
				if(resp.data?.status == "active") {
					rtn.success = true
					rtn.data = dropletToWorkloadResponse(resp.data)
					pending = false
				} else if(resp.msg == 'failed') {
					rtn.msg = resp.msg
					pending = false
				}
			}
			attempts++
			if (attempts > 15) {
				pending = false
			}
		}

		return rtn
	}

	ServiceResponse<WorkloadResponse> powerOffServer(String apiKey, String dropletId) {
		log.debug("powerOffServer: $dropletId")
		return apiService.performDropletAction(apiKey, dropletId, 'power_off')
	}

	protected WorkloadResponse dropletToWorkloadResponse(Map droplet, Map opts=[:]) {
		WorkloadResponse workloadResponse = new WorkloadResponse()
		workloadResponse.externalId = droplet?.id

		// networks
		def publicNetwork = droplet?.networks?.v4?.find {
			it.type == 'public'
		}
		def privateNetwork = droplet?.networks?.v4?.find {
			it.type == 'private'
		}
		def publicIp = publicNetwork?.ip_address
		def privateIp = privateNetwork?.ip_address ?: publicIp
		workloadResponse.publicIp = publicIp
		workloadResponse.privateIp = privateIp

		if(opts?.containsKey('installAgent')) {
			workloadResponse.installAgent = opts.installAgent
		}
		if(opts?.containsKey('noAgent')) {
			workloadResponse.noAgent = opts.noAgent
		}
		if(opts?.containsKey('createUsers')) {
			workloadResponse.createUsers = opts.createUsers
		}

		return workloadResponse
	}

	protected HostResponse workloadResponseToHostResponse(WorkloadResponse workloadResponse) {
		HostResponse hostResponse = new HostResponse([
			unattendCustomized: workloadResponse.unattendCustomized,
			externalId        : workloadResponse.externalId,
			publicIp          : workloadResponse.publicIp,
			privateIp         : workloadResponse.privateIp,
			installAgent      : workloadResponse.installAgent,
			noAgent           : workloadResponse.noAgent,
			createUsers       : workloadResponse.createUsers,
			success           : workloadResponse.success,
			customized        : workloadResponse.customized,
			licenseApplied    : workloadResponse.licenseApplied,
			poolId            : workloadResponse.poolId,
			hostname          : workloadResponse.hostname,
			message           : workloadResponse.message,
			skipNetworkWait   : workloadResponse.skipNetworkWait
		])

		return hostResponse
	}

	protected ComputeServer saveAndGet(ComputeServer server) {
		morpheus.computeServer.save([server]).blockingGet()
		return morpheus.computeServer.get(server.id).blockingGet()
	}

	protected cleanInstanceName(name) {
		def rtn = name.replaceAll(/[^a-zA-Z0-9\.\-]/,'')
		return rtn
	}
}

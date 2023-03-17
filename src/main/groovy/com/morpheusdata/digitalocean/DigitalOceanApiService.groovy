package com.morpheusdata.digitalocean

import com.morpheusdata.response.ServiceResponse
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.HttpApiClient.RequestOptions

@Slf4j
class DigitalOceanApiService {
	protected static final String DIGITAL_OCEAN_ENDPOINT = 'https://api.digitalocean.com'

	HttpApiClient apiClient

	DigitalOceanApiService() {
		this.apiClient = new HttpApiClient()
	}

	ServiceResponse getDroplet(String apiKey, String dropletId) {
		ServiceResponse rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets/${dropletId}"
		ServiceResponse response = internalGetApiRequest(apiKey, apiPath)
		log.info("response: ${response}")
		if (response.success) {
			rtn.success = true
			rtn.data = response.data?.droplet
			rtn.results = response.data
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
		}

		return rtn
	}

	ServiceResponse createDroplet(String apiKey, Map dropletConfig) {
		ServiceResponse rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets"

		ServiceResponse response = internalPostApiRequest(apiKey, apiPath, dropletConfig)
		if(response.success) {
			rtn.success = true
			rtn.results = response.data
			rtn.data = response.data.droplet
		} else {
			rtn.errorCode = results.errorCode
			rtn.results = rtn.data
		}

		return rtn
	}

	ServiceResponse deleteDroplet(String apiKey, String dropletId) {
		def rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets/${dropletId}"
		ServiceResponse response = internalDeleteApiRequest(apiKey, apiPath)
		log.debug("deleteDroplet response: ${response}")
		if(response.success) {
			rtn.success = true
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
			log.debug("deleteDroplet error: ${rtn.errorCode}, results: ${rtn.results}")
		}

		return rtn
	}

	ServiceResponse performDropletAction(String apiKey, String dropletId, String action, Map config=null) {
		ServiceResponse rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets/${dropletId}/actions"
		Map body = [type: action]
		if(config) {
			body = body + config
		}
		ServiceResponse results = internalPostApiRequest(apiKey, apiPath, body)
		if (results.success) {
			rtn = checkActionComplete(apiKey, results.data.action.id)
		} else {
			rtn.content = results.content
			rtn.errorCode = results.erroCode
			rtn.msg = results.errorCode
			rtn.error = response.data
		}

		return rtn
	}

	ServiceResponse checkActionComplete(String apiKey, Integer actionId) {
		try {
			def pending = true
			def attempts = 0
			while (pending) {
				log.debug("waiting for action complete...")
				sleep(1000l * 10l)
				ServiceResponse actionDetail = getAction(apiKey, actionId)
				if (actionDetail.success == true && actionDetail?.data?.status) {
					def tmpState = actionDetail.data.status
					if (tmpState == 'completed' || tmpState == 'failed') {
						return actionDetail
					}
				}
				attempts++
				if (attempts > 60) {
					pending = false
				}
			}
		} catch (e) {
			log.debug("An Exception Has Occurred: ${e.message}")
		}
		return new ServiceResponse(success: false, msg: 'Too many failed attempts to check Droplet action status')
	}

	ServiceResponse getAction(String apiKey, String actionId) {
		def rtn = ServiceResponse.prepare()
		String apiPath = "/v2/actions/${actionId}"
		ServiceResponse response = internalGetApiRequest(apiKey, apiPath)
		log.debug("getAction response: ${response}")
		if(response.success) {
			rtn.success = true
			rtn.results = response.data
			rtn.data = response.data.action
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
		}

		return rtn
	}

	ServiceResponse listDropletSnapshots(String apiKey, String dropletId) {
		ServiceResponse rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets/${dropletId}/snapshots"

		ServiceResponse response = internalGetApiRequest(apiKey, apiPath)
		log.debug("listDropletSnapshots response: ${response}")
		if(response.success) {
			rtn.success = true
			rtn.results = response.data
			rtn.data = response.data.snapshots
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
		}

		return rtn
	}

	ServiceResponse createSnapshot(String apiKey, String dropletId, String snapshotName) {
		def rtn = ServiceResponse.prepare()
		String apiPath = "/v2/droplets/${dropletId}/actions"
		Map body = [
			'type':'snapshot',
			'name': snapshotName
		]
		ServiceResponse response = internalPostApiRequest(apiKey, apiPath, body)
		log.debug("createSnapshot response: ${response}")
		if (response.success) {
			rtn.success = true
			rtn.data = response.data.action
			rtn.results = response.data
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
		}

		return rtn
	}

	ServiceResponse deleteSnapshot(String apiKey, String snapshotId){
		def rtn = ServiceResponse.prepare()
		String apiPath = "/v2/snapshots/${snapshotId}"
		ServiceResponse response = internalDeleteApiRequest(apiKey, apiPath)
		log.debug("deleteSnapshot response: ${response}")
		if(response.success) {
			rtn.success = true
		} else {
			rtn.success = false
			rtn.errorCode = response.errorCode
			rtn.results = response.data
			log.debug("deleteSnapshot error: ${rtn.errorCode}, results: ${rtn.results}")
		}

		return rtn
	}

	private ServiceResponse internalGetApiRequest(String apiKey, String path, Map queryParams=null, Map headers=null) {
		internalApiRequest(apiKey, path, 'GET', null, queryParams, headers)
	}

	private ServiceResponse internalPostApiRequest(String apiKey, String path, Map body=null, Map queryParams=null, Map headers=null) {
		internalApiRequest(apiKey, path, 'POST', body, queryParams, headers)
	}

	private ServiceResponse internalPatchApiRequest(String apiKey, String path, Map body=null, Map queryParams=null, Map headers=null) {
		internalApiRequest(apiKey, path, 'PATCH', body, queryParams, headers)
	}

	private ServiceResponse internalDeleteApiRequest(String apiKey, String path, Map queryParams=null, Map headers=null) {
		internalApiRequest(apiKey, path, 'DELETE', null, queryParams, headers)
	}

	private ServiceResponse internalApiRequest(String apiKey, String path, String requestMethod='GET', Map body=null, Map queryParams=null, Map addHeaders=null) {
		def rtn = ServiceResponse.prepare()
		try {
			RequestOptions requestOptions = new RequestOptions(apiToken: apiKey, contentType: 'application/json')
			if(body) {
				requestOptions.body = body
			}
			if(queryParams) {
				requestOptions.queryParams = queryParams
			}
			requestOptions.headers = ['Accept':'application/json', 'ContentType':'application/json']
			if(addHeaders) {
				addHeaders.each {
					requestOptions.headers[it.key] = it.value
				}
			}
			rtn = apiClient.callJsonApi(DIGITAL_OCEAN_ENDPOINT, path, requestOptions, requestMethod)
		} catch(e) {
			log.error("error during api request {}: {}", path, e, e)
		}
		return rtn
	}

	protected Map makeApiCall(HttpRequestBase http, String apiKey) {
		CloseableHttpClient client = HttpClients.createDefault()
		try {
			http.addHeader("Authorization", "Bearer ${apiKey}")
			http.addHeader("Content-Type", "application/json")
			http.addHeader("Accept", "application/json")
			def resp = client.execute(http)
			try {
				log.debug "resp: ${resp}"
				String responseContent
				if(resp?.entity) {
					responseContent = EntityUtils.toString(resp?.entity)
				}
				log.debug "content: $responseContent"
				JsonSlurper slurper = new JsonSlurper()
				def json = responseContent ? slurper.parseText(responseContent) : null

				return [resp: resp, json: json]
			} catch (Exception e) {
				log.debug "Error making DO API call: ${e.message}"
			} finally {
				resp.close()
			}
		} catch (Exception e) {
			log.debug "Http Client error: ${e.localizedMessage}"
			e.printStackTrace()
		} finally {
			client.close()
		}
	}

	protected List makePaginatedApiCall(String apiKey, String path, String resultKey, Map queryParams) {
		List resultList = []
		def pageNum = 1
		def perPage = 10
		Map query = [per_page: "${perPage}", page: "${pageNum}"]
		query += queryParams

		URIBuilder uriBuilder = new URIBuilder(DIGITAL_OCEAN_ENDPOINT)
		uriBuilder.path = path
		query.each { k, v ->
			uriBuilder.addParameter(k, v)
		}

		HttpGet httpGet = new HttpGet(uriBuilder.build())
		Map respMap = makeApiCall(httpGet, apiKey)
		resultList += respMap?.json?."$resultKey"
		log.debug "resultList: $resultList"
		def theresMore = respMap?.json?.links?.pages?.next ? true : false
		while (theresMore) {
			pageNum++
			query.page = "${pageNum}"
			uriBuilder.parameters = []
			query.each { k, v ->
				uriBuilder.addParameter(k, v)
			}
			httpGet = new HttpGet(uriBuilder.build())
			def moreResults = makeApiCall(httpGet, apiKey)
			log.debug "moreResults: $moreResults"
			resultList += moreResults.json[resultKey]
			theresMore = moreResults.json.links.pages.next ? true : false
		}

		return resultList
	}

}

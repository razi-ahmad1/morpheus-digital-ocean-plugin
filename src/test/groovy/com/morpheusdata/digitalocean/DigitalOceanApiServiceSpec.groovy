package com.morpheusdata.digitalocean

import com.morpheusdata.response.ProvisionResponse
import spock.lang.Specification
import spock.lang.Subject

class DigitalOceanApiServiceSpec extends Specification {

	@Subject
	DigitalOceanApiService service

	def setup() {
		service = new DigitalOceanApiService()
	}

	void "dropletToProvisionResponse"() {
		given:
		def droplet = [
				id      : '1111',
				networks: [
						v4: [
								[ip_address: '10.10.10.10', type: 'public'],
								[ip_address: '192.168.0.10', type: 'private'],
						]
				]
		]
		ProvisionResponse expected = new ProvisionResponse(externalId: '1111', publicIp: '10.10.10.10', privateIp: '192.168.0.10', success:true)

		when:
		def resp = service.dropletToProvisionResponse(droplet)

		then:
		resp.externalId == expected.externalId
		resp.publicIp == expected.publicIp
		resp.privateIp == expected.privateIp
	}
}

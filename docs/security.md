# Secure Communication over TLS
uplink communicates with the Bytebeam platform over MQTT+TLS, ensuring application data is encrypted in transit. Bytebeam also uses SSL certificates to authenticate connections and identify devices, and uplink expects users to provide it with an auth file with the `-a` flag. This file contains certificate information for the device and the certifying authority in the following JSON format:
```js
{
    "project_id": "xxxx",
    "device_id": "1234",
    "broker": "example.com",
    "port": 8883,
    "authentication": {
        "ca_certificate": "...",
        "device_certificate": "...",
        "device_private_key": "..."
    }
}
```

## Using uplink without TLS
One could use uplink with a broker of their choice, without having to configure TLS. This can be achieved by simply omitting the authentication field in the above JSON and customizing it for use with their setup. i.e:
```js
{
    "project_id": "xxxx",
    "device_id": "1234",
    "broker": "example.com",
    "port": 8883
}
```

## Configuring uplink for use with TLS
- To use uplink, you need to provide it with an authenication file, let us first source it from the Bytebeam platform.
    1. Login to your account on [demo.bytebeam.io](https://demo.bytebeam.io) and ensure that you are in the right organization.
    2. From within the "Device Management" UI, select the "Devices" tab and press on the "Create Device" button.
    3. Press "Submit" and your browser will download the newly created device's authentication file with a JSON format mentioned in the introductory section.
- If you want to use your own broker with a TLS setup, follow the steps mentioned in the [Provisioning your own certificates](#Provisioning-your-own-certificates) section.
- Once you have downloaded the file, say `auth.json`, you can use it to run an instance of uplink with the following command:

## Provisioning your own certificates
If you are using a self-hosted broker setup, we recommend setting up TLS by following the given steps.
1. Download and install the [provision release](https://github.com/bytebeamio/provision/releases) binary of your choice, ensure you have any requirements for it installed.
2. Create a CA certificate, the server and device certificates, an example for the commands to be used:
```sh
provision ca
provision server --ca ca.cert.pem --cakey ca.key.pem --domain <domain>
provision client --ca ca.cert.pem --cakey ca.key.pem --device <device-id> --tenant <project-id> --bits 2048
```
3. Now that you have the necessary certificates and keys, setup your broker with the server keys and certificate and do the following to the rest
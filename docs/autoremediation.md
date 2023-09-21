# Auto Remediation
PSC comes out-of-the-box with automated exception handling and remediation mechanisms that enable clients to self-recover from known exceptions thrown by the backend client.

## Motivation
No matter how robust a client library is, client-server interactions in a distributed environment are still prone to errors. Often times, these issues can be solved via simple retries or client resets. However, in native client libraries, these exceptions are often handed off to the application layer. This leaves the handling of these exceptions to the application owners who may or may not have context on how best to address them. In many cases, they are simply not handled, causing applications to crash or restart. This increases KTLO burden for application and platform teams alike.

Due to its design, PSC is uniquely positioned between the native client and application layers to handle these errors gracefully. Common PubSub client/server connectivity issues that have a known remediation on the client side, such as client reset or simple retries, can be done underneath the hood by PSC without causing disruption to the application layer.

## Usage
To enable auto remediation in your PSC client, simply set this configuration:

```
psc.auto.resolution.enabled=true
```

To specify the maximum number of retries that PSC will attempt in handling a retriable exception (e.g. 5):

```
psc.auto.resolution.retry.count=5
```
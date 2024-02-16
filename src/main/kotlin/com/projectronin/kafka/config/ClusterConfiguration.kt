package com.projectronin.kafka.config

@Deprecated("Library has been replaced by ronin-common kafka")
interface ClusterConfiguration {
    val bootstrapServers: String
    val securityProtocol: String?
    val saslMechanism: String?
    val saslJaasConfig: String?
    val saslUsername: String?
    val saslPassword: String?
}

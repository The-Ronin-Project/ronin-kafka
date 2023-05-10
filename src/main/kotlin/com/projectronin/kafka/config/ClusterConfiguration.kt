package com.projectronin.kafka.config

interface ClusterConfiguration {
    val bootstrapServers: String
    val securityProtocol: String?
    val saslMechanism: String?
    val saslJaasConfig: String?
    val saslUsername: String?
    val saslPassword: String?
}

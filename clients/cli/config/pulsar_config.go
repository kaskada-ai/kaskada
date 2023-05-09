package config

import (
	"github.com/spf13/cobra"

	apiv1alpha "github.com/kaskada-ai/kaskada/gen/proto/go/kaskada/kaskada/v1alpha"
)

type PulsarConfig struct {
	// The Pulsar protocol URL for the cluster.
	//
	// Defaults to "pulsar://127.0.0.1:6650".
	BrokerServiceURL *string `json:"broker_service_url"`

	// The Pulsar admin REST URL for the cluster.
	//
	// Defaults to "http://127.0.0.1:8080"
	AdminServiceURL *string `json:"admin_service_url"`

	// Authentication plugin to use.
	//
	// Defaults to "org.apache.pulsar.client.impl.auth.AuthenticationToken"
	AuthPlugin *string `json:"auth_plugin"`

	// Authentication parameters.
	// e.g. "token:xxx"
	AuthParams *string `json:"auth_params"`

	// The topic tenant within the instance.
	//
	// Defaults to "public".
	Tenant *string `json:"tenant"`

	// The administrative unit of topics, which acts as a grouping
	// mechanism for related topics.
	//
	// Defaults to "default".
	Namespace *string `json:"namespace"`

	// The final part of the topic url.
	//
	// Defaults to a randomly generated uuid.
	TopicName *string `json:"topic_name"`
}

func NewPulsarConfig() PulsarConfig {
	return PulsarConfig{
		BrokerServiceURL: new(string),
		AdminServiceURL:  new(string),
		AuthPlugin:       new(string),
		AuthParams:       new(string),
		Tenant:           new(string),
		Namespace:        new(string),
		TopicName:        new(string),
	}
}

func (c PulsarConfig) AddFlagsToCommand(cmd *cobra.Command) {
	cmd.Flags().StringVar(c.BrokerServiceURL, "pulsar-broker-service-url", "pulsar://127.0.0.1:6650", "The Pulsar protocol URL for the cluster.")
	cmd.Flags().StringVar(c.AdminServiceURL, "pulsar-admin-service-url", "http://127.0.0.1", "The Pulsar admin REST URL for the cluster.")
	cmd.Flags().StringVar(c.AuthPlugin, "pulsar-auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken", "The authentication plugin to use.")
	cmd.Flags().StringVar(c.AuthParams, "pulsar-auth-params", "", "The authentication parameters. (example \"token:xxx\")")
	cmd.Flags().StringVar(c.Tenant, "pulsar-tenant", "public", "The topic tenant within the instance.")
	cmd.Flags().StringVar(c.Namespace, "pulsar-namespace", "default", "The administrative unit of topics, which acts as a grouping mechanism for related topics.")
	cmd.Flags().StringVar(c.TopicName, "pulsar-topic-name", "", "The final part of the topic url. (default \"<random_uuid>\")")

	cmd.MarkFlagsRequiredTogether("pulsar-broker-service-url", "pulsar-admin-service-url")
}

func (c PulsarConfig) ToProto() (*apiv1alpha.PulsarConfig, error) {
	return nil, nil
}

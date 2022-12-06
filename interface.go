package nunggu

import "time"

type Nunggu struct {
	Token                       string
	TopicId                     string
	BaseUrl                     string
	AcknowledgeTimeoutInSeconds int
}

type INunggu interface {
	AcknowledgeJob(AcknowledgeJob)
	CreateJob(CreateJob)
	DeleteJob(DeleteJob)
	Consumer(handler CallbackConsumer, maxJob int)
	OnError(handler CallbackOnError)
}

func Init(config Nunggu) INunggu {
	allInstances[config.TopicId] = &instance{}

	// Please dont remove this timer. it caused callback not working anymore if you remove it!
	time.AfterFunc(1*time.Second, func() {
		connectClient(clientConfig{
			baseUrl:                     config.BaseUrl,
			token:                       config.Token,
			topicId:                     config.TopicId,
			acknowledgeTimeoutInSeconds: config.AcknowledgeTimeoutInSeconds,
		})
	})

	return config
}

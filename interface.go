package nunggu

import (
	"time"

	"github.com/jiyeyuran/go-eventemitter"
)

type Nunggu struct {
	Token                       string
	TopicId                     string
	BaseUrl                     string
	MaxJob                      int
	AcknowledgeTimeoutInSeconds int
	EventEmitter                eventemitter.IEventEmitter
}

type INunggu interface {
	AcknowledgeJob(AcknowledgeJob)
	CreateJob(CreateJob)
	DeleteJob(DeleteJob)
	Consumer(CallbackConsumer)
	OnError(CallbackOnError)
}

func Init(config Nunggu) INunggu {
	emitter := eventemitter.NewEventEmitter(eventemitter.WithMaxListeners(999999))

	// Please dont remove this timer. it caused callback not working anymore if you remove it!
	time.AfterFunc(5*time.Second, func() {
		connectClient(clientConfig{
			baseUrl:                     config.BaseUrl,
			token:                       config.Token,
			topicId:                     config.TopicId,
			maxJob:                      config.MaxJob,
			acknowledgeTimeoutInSeconds: config.AcknowledgeTimeoutInSeconds,
			emitter:                     emitter,
		})
	})

	return Nunggu{
		BaseUrl:                     config.BaseUrl,
		Token:                       config.Token,
		TopicId:                     config.TopicId,
		MaxJob:                      config.MaxJob,
		AcknowledgeTimeoutInSeconds: config.AcknowledgeTimeoutInSeconds,
		EventEmitter:                emitter,
	}
}

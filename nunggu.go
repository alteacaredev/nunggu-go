package nunggu

import (
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jiyeyuran/go-eventemitter"
)

type (
	CallbackConsumer func(ConsumerData)
	CallbackOnError  func(error)

	clientConfig struct {
		baseUrl                     string
		token                       string
		topicId                     string
		maxJob                      int
		acknowledgeTimeoutInSeconds int
		emitter                     eventemitter.IEventEmitter
	}
	ConsumerData struct {
		JobId   string      `json:"job_id"`
		Key     string      `json:"key"`
		Attempt int         `json:"attempt"`
		JobData interface{} `json:"job_data"`
	}

	IncomingMessage struct {
		Status  bool         `json:"status"`
		Message string       `json:"message"`
		Type    string       `json:"type"`
		Data    ConsumerData `json:"data"`
	}
	AcknowledgeJob struct {
		JobId   string
		Status  bool
		Message string
		Data    interface{}
	}
	CreateJob struct {
		Key        string
		StartTime  time.Time
		Data       interface{}
		MaxAttempt int
	}
	DeleteJob struct {
		JobId string
		Key   string
	}
)

func connectClient(cc clientConfig) {
	url, _ := url.Parse(cc.baseUrl)
	queries := url.Query()
	queries.Add("token", cc.token)
	queries.Add("topic_id", cc.topicId)
	if cc.maxJob > 0 {
		queries.Add("max_job", strconv.Itoa(cc.maxJob))
	}
	if cc.acknowledgeTimeoutInSeconds > 0 {
		queries.Add("acknowledge_timeout_in_seconds", strconv.Itoa(cc.acknowledgeTimeoutInSeconds))
	}
	url.RawQuery = queries.Encode()

	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		cc.emitter.SafeEmit("ON_ERROR", err)

		time.AfterFunc(1*time.Second, func() {
			connectClient(cc)
		})

		return
	}

	defer c.Close()

	cc.emitter.On("ACKNOWLEDGE_JOB", func(acknowledgeJob AcknowledgeJob) {
		payload := map[string]interface{}{
			"type": "ACKNOWLEDGE_JOB",
			"data": map[string]interface{}{
				"job_id":  acknowledgeJob.JobId,
				"status":  acknowledgeJob.Status,
				"message": acknowledgeJob.Message,
				"data":    acknowledgeJob.Data,
			},
		}

		c.WriteJSON(payload)
	})

	cc.emitter.On("CREATE_JOB", func(createJob CreateJob) {
		data := map[string]interface{}{
			"key":        createJob.Key,
			"start_time": createJob.StartTime.Format("2006-01-02 15:04:05 -07:00"),
			"data":       createJob.Data,
		}

		if createJob.MaxAttempt > 0 {
			data["max_attempt"] = createJob.MaxAttempt
		}

		payload := map[string]interface{}{
			"type": "CREATE_JOB",
			"data": data,
		}

		c.WriteJSON(payload)
	})

	cc.emitter.On("DELETE_JOB", func(deleteJob DeleteJob) {
		data := map[string]interface{}{}

		if deleteJob.JobId != "" {
			data["job_ref_id"] = deleteJob.JobId
		}

		if deleteJob.Key != "" {
			data["key"] = deleteJob.Key
		}

		payload := map[string]interface{}{
			"type": "DELETE_JOB",
			"data": data,
		}

		c.WriteJSON(payload)
	})

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			cc.emitter.SafeEmit("ON_ERROR", err)

			time.AfterFunc(1*time.Second, func() {
				connectClient(cc)
			})
			break
		}

		handleMessage(cc.emitter, message)
	}

	c.SetPingHandler(func(string) error {
		err := c.WriteMessage(websocket.PongMessage, []byte("keepalive"))

		if err != nil {
			cc.emitter.SafeEmit("ON_ERROR", err)
		}

		return nil
	})
}

func handleMessage(emitter eventemitter.IEventEmitter, message []byte) {
	incomingMessage := IncomingMessage{}
	err := json.Unmarshal(message, &incomingMessage)
	if err != nil {
		emitter.SafeEmit("ON_ERROR", err)
	}

	switch incomingMessage.Type {
	case "NEW_JOB":
		{
			emitter.SafeEmit("CONSUMER", incomingMessage.Data)
		}
	case "ERROR":
		{
			emitter.SafeEmit("ON_ERROR", errors.New(incomingMessage.Message))
		}
	}
}

func (i Nunggu) Consumer(callback CallbackConsumer) {
	i.EventEmitter.On("CONSUMER", func(consumerData ConsumerData) {
		callback(consumerData)
	})
}

func (i Nunggu) OnError(callback CallbackOnError) {
	i.EventEmitter.On("ON_ERROR", func(err error) {
		callback(err)
	})
}

func (i Nunggu) AcknowledgeJob(acknowledgeJob AcknowledgeJob) {
	if acknowledgeJob.JobId != "" {
		i.EventEmitter.SafeEmit("ACKNOWLEDGE_JOB", acknowledgeJob)
	}
}

func (i Nunggu) CreateJob(createJob CreateJob) {
	if createJob.Key != "" && !createJob.StartTime.IsZero() {
		i.EventEmitter.SafeEmit("CREATE_JOB", createJob)
	}
}

func (i Nunggu) DeleteJob(deleteJob DeleteJob) {
	if deleteJob.JobId != "" || deleteJob.Key != "" {
		i.EventEmitter.SafeEmit("DELETE_JOB", deleteJob)
	}
}

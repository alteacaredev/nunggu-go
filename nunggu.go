package nunggu

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jiyeyuran/go-eventemitter"
)

var allInstances = make(map[string]*instance)

type (
	CallbackConsumer func(ConsumerData)
	CallbackOnError  func(error)

	clientConfig struct {
		baseUrl                     string
		token                       string
		topicId                     string
		acknowledgeTimeoutInSeconds int
	}
	instance struct {
		clientConnected  bool
		haveConsumer     bool
		maxJob           int
		callbackConsumer CallbackConsumer
		callbackError    CallbackOnError
		emitter          eventemitter.IEventEmitter
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
	allInstances[cc.topicId].emitter = eventemitter.NewEventEmitter()

	fmt.Printf("Connecting Nunggu Client... (%s) \n", time.Now().Format("02/01/2006 15:04:05 -07:00"))

	url, _ := url.Parse(cc.baseUrl)
	queries := url.Query()
	queries.Add("token", cc.token)
	queries.Add("topic_id", cc.topicId)
	if cc.acknowledgeTimeoutInSeconds > 0 {
		queries.Add("acknowledge_timeout_in_seconds", strconv.Itoa(cc.acknowledgeTimeoutInSeconds))
	}
	url.RawQuery = queries.Encode()

	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	registerListeners(c, cc.topicId)
	if err != nil {
		allInstances[cc.topicId].emitter.SafeEmit("ON_ERROR", err)

		time.AfterFunc(1*time.Second, func() {
			connectClient(cc)
		})

		return
	}

	defer c.Close()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			allInstances[cc.topicId].emitter.SafeEmit("ON_ERROR", err)

			time.AfterFunc(1*time.Second, func() {
				connectClient(cc)
			})
			break
		}

		handleMessage(cc.topicId, message)
	}

	c.SetPingHandler(func(string) error {
		err := c.WriteMessage(websocket.PongMessage, []byte("keepalive"))

		if err != nil {
			allInstances[cc.topicId].emitter.SafeEmit("ON_ERROR", err)
		}

		return nil
	})
}

func registerListeners(c *websocket.Conn, topicId string) {
	allInstances[topicId].emitter.On("ACKNOWLEDGE_JOB", func(acknowledgeJob AcknowledgeJob) {
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

	allInstances[topicId].emitter.On("STATUS", func(status bool) {
		statusStr := "connected"
		if !status {
			statusStr = "disconnected"
		}

		allInstances[topicId].clientConnected = status
		allInstances[topicId].emitter.SafeEmit("HAVE_CONSUMER", allInstances[topicId].haveConsumer, allInstances[topicId].maxJob)

		fmt.Printf("Nunggu Client %s with topic id \"%s\" at (%s) \n ", statusStr, topicId, time.Now().Format("02/01/2006 15:04:05 -07:00"))
	})

	allInstances[topicId].emitter.On("HAVE_CONSUMER", func(haveConsumer bool, maxJob int) {
		data := map[string]interface{}{
			"have_consumer": haveConsumer,
			"max_job":       3,
		}
		if maxJob > 0 {
			data["max_job"] = maxJob
		}
		payload := map[string]interface{}{
			"type": "HAVE_CONSUMER",
			"data": data,
		}

		c.WriteJSON(payload)
	})

	allInstances[topicId].emitter.On("CREATE_JOB", func(createJob CreateJob) {
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

	allInstances[topicId].emitter.On("DELETE_JOB", func(deleteJob DeleteJob) {
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

	allInstances[topicId].emitter.On("CONSUMER", func(consumerData ConsumerData) {
		if allInstances[topicId].callbackConsumer != nil {
			allInstances[topicId].callbackConsumer(consumerData)
		}
	})

	allInstances[topicId].emitter.On("ON_ERROR", func(err error) {
		if allInstances[topicId].callbackError == nil {
			fmt.Printf("Error: %v (%s) \n", err.Error(), time.Now().Format("02/01/2006 15:04:05 -07:00"))
			return
		}

		allInstances[topicId].callbackError(err)
	})
}

func handleMessage(topicId string, message []byte) {
	incomingMessage := IncomingMessage{}
	err := json.Unmarshal(message, &incomingMessage)
	if err != nil {
		allInstances[topicId].emitter.SafeEmit("ON_ERROR", err)
	}

	switch incomingMessage.Type {
	case "NEW_JOB":
		{
			allInstances[topicId].emitter.SafeEmit("CONSUMER", incomingMessage.Data)
		}
	case "ERROR":
		{
			allInstances[topicId].emitter.SafeEmit("ON_ERROR", errors.New(incomingMessage.Message))
		}
	case "STATUS":
		{
			allInstances[topicId].clientConnected = incomingMessage.Status

			allInstances[topicId].emitter.SafeEmit("STATUS", incomingMessage.Status)
		}
	}
}

func (i Nunggu) Consumer(callback CallbackConsumer, pMaxJob int) {
	allInstances[i.TopicId].haveConsumer = true
	allInstances[i.TopicId].maxJob = pMaxJob
	allInstances[i.TopicId].callbackConsumer = callback
}

func (i Nunggu) OnError(callback CallbackOnError) {
	allInstances[i.TopicId].callbackError = callback
}

func (i Nunggu) AcknowledgeJob(acknowledgeJob AcknowledgeJob) {
	for {
		if allInstances[i.TopicId].clientConnected {
			break
		}
	}

	if acknowledgeJob.JobId != "" {
		allInstances[i.TopicId].emitter.SafeEmit("ACKNOWLEDGE_JOB", acknowledgeJob)
	}
}

func (i Nunggu) CreateJob(createJob CreateJob) {
	for {
		if allInstances[i.TopicId].clientConnected {
			break
		}
	}

	if createJob.Key != "" && !createJob.StartTime.IsZero() {
		allInstances[i.TopicId].emitter.SafeEmit("CREATE_JOB", createJob)
	}
}

func (i Nunggu) DeleteJob(deleteJob DeleteJob) {
	for {
		if allInstances[i.TopicId].clientConnected {
			break
		}
	}

	if deleteJob.JobId != "" || deleteJob.Key != "" {
		allInstances[i.TopicId].emitter.SafeEmit("DELETE_JOB", deleteJob)
	}
}

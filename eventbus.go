/**
 * Copyright (c) 2018 KISTLER INSTRUMENTE AG, Winterthur, Switzerland
 *
 * @file eventbus.go
 *
 * Created on: Sep 21, 2018
 * Project: eh-kafka
 * Description: Event Bus Kafka for Event Horizont
 *
 * This file is part of eh-kafka.
 *
 * eh-kafka is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * eh-kafka is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with eh-kafka. If not, see <http://www.gnu.org/licenses/>.
 */

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sarama "github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"

	"github.com/globalsign/mgo/bson"

	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
func ErrCouldNotMarshalEvent(err error) error {
	return fmt.Errorf("could not marshal event: %v", err.Error())
}

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into BSON.
func ErrCouldNotUnmarshalEvent(err error) error {
	return fmt.Errorf("could not unmarshal event: %v", err.Error())
}

// ErrCouldNotPublishEvent is when kafka-client cannot send event to kafka
func ErrCouldNotPublishEvent(err error) error {
	return fmt.Errorf("could not unmarshal event: %v", err.Error())
}

// EventBus is an event bus that notifies registered EventHandlers of
// published events. It will use the SimpleEventHandlingStrategy by default.
type EventBus struct {
	brokers            []string
	config             *sarama.Config
	producerTopicFunc  func(event eh.Event) string
	consumerTopicsFunc func(event eh.EventHandler) []string
	timeout            time.Duration

	producer sarama.SyncProducer

	registered   map[eh.EventHandlerType]struct{}
	registeredMu sync.RWMutex
	errCh        chan eh.EventBusError
}

// NewEventBus creates a EventBus.
func NewEventBus(brokers []string, config *sarama.Config, timeout time.Duration, producerTopicFunc func(event eh.Event) string, consumerTopicsFunc func(event eh.EventHandler) []string) (*EventBus, error) {
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &EventBus{
		brokers:            brokers,
		config:             config,
		timeout:            timeout,
		producerTopicFunc:  producerTopicFunc,
		consumerTopicsFunc: consumerTopicsFunc,
		producer:           producer,
		registered:         map[eh.EventHandlerType]struct{}{},
		errCh:              make(chan eh.EventBusError, 100),
	}, nil
}

// PublishEvent publishes an event to all handlers capable of handling it.
func (b *EventBus) PublishEvent(ctx context.Context, event eh.Event) error {
	e := evt{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		rawData, err := bson.Marshal(event.Data())
		if err != nil {
			return errors.New("could not marshal event data: " + err.Error())
		}
		e.RawData = bson.Raw{Kind: 3, Data: rawData}
	}

	// Marshal the event (using BSON for now).
	data, err := bson.Marshal(e)
	if err != nil {
		return errors.New("could not marshal event: " + err.Error())
	}
	_, _, err = b.producer.SendMessage(&sarama.ProducerMessage{
		Topic: b.producerTopicFunc(event),
		Value: sarama.StringEncoder(data),
	})

	if err != nil {
		return ErrCouldNotPublishEvent(err)
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(m eh.EventMatcher, h eh.EventHandler) {
	consumer := b.subscription(m, h, false)
	b.runHandle(m, h, consumer)
}

// AddObserver implements the AddObserver method of the eventhorizon.EventBus interface.
func (b *EventBus) AddObserver(m eh.EventMatcher, h eh.EventHandler) {
	consumer := b.subscription(m, h, true)
	b.runHandle(m, h, consumer)
}

// Errors returns an error channel where async handling errors are sent.
func (b *EventBus) Errors() <-chan eh.EventBusError {
	return b.errCh
}

// Checks the matcher and handler and gets the event subscription.
func (b *EventBus) subscription(m eh.EventMatcher, h eh.EventHandler, observer bool) *cluster.Consumer {
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()

	if m == nil {
		panic("matcher can't be nil")
	}
	if h == nil {
		panic("handler can't be nil")
	}
	if _, ok := b.registered[h.HandlerType()]; ok {
		panic(fmt.Sprintf("multiple registrations for %s", h.HandlerType()))
	}
	b.registered[h.HandlerType()] = struct{}{}

	clientID := eh.NewUUID()
	group := string(h.HandlerType())
	if observer { // Generate unique ID for each observer.
		group = fmt.Sprintf("%s-%s", group, clientID)
	}

	config := cluster.NewConfig()
	config.Config = *b.config
	config.Config.ClientID = clientID.String()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(b.brokers, group, b.consumerTopicsFunc(h), config)
	if err != nil {
		panic(fmt.Sprintf("cannot create consumer for %v: %v", h.HandlerType(), err))
	}
	return consumer
}

// wait for for first event from handle because registering to kafka is too slow
func (b *EventBus) runHandle(m eh.EventMatcher, h eh.EventHandler, consumer *cluster.Consumer) {
	sync := make(chan interface{})
	go func(sync chan interface{}) {
		b.handle(m, h, consumer, func() {
			if sync != nil {
				close(sync)
				sync = nil
			}
		})
	}(sync)
	select {
	case <-sync:
	case <-time.After(b.timeout):
		panic("cannot run handle: timeout")
	}
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(m eh.EventMatcher, h eh.EventHandler, consumer *cluster.Consumer, rebalanceOk func()) {
	run := true
	for run == true {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				b.handleMessage(m, h, consumer, msg)
			}
		case ntf := <-consumer.Notifications():
			if ntf.Type == cluster.RebalanceOK {
				rebalanceOk()
			}
		case err := <-consumer.Errors():
			select {
			case b.errCh <- eh.EventBusError{Err: errors.New("could not receive: " + err.Error())}:
			default:
			}
		}
	}
}

func (b *EventBus) handleMessage(m eh.EventMatcher, h eh.EventHandler, consumer *cluster.Consumer, msg *sarama.ConsumerMessage) {
	// Manually decode the raw BSON event.
	data := bson.Raw{
		Kind: 3,
		Data: msg.Value,
	}
	var e evt
	if err := data.Unmarshal(&e); err != nil {
		select {
		case b.errCh <- eh.EventBusError{Err: errors.New("could not unmarshal event: " + err.Error())}:
		default:
		}
		return
	}

	// Create an event of the correct type.
	if data, err := eh.CreateEventData(e.EventType); err == nil {
		// Manually decode the raw BSON event.
		if err := e.RawData.Unmarshal(data); err != nil {
			select {
			case b.errCh <- eh.EventBusError{Err: errors.New("could not unmarshal event data: " + err.Error())}:
			default:
			}
			return
		}

		// Set concrete event and zero out the decoded event.
		e.data = data
		e.RawData = bson.Raw{}
	}

	event := event{evt: e}
	ctx := eh.UnmarshalContext(e.Context)

	if !m(event) {
		consumer.MarkOffset(msg, "")
		return
	}

	// Notify all observers about the event.
	if err := h.HandleEvent(ctx, event); err != nil {
		select {
		case b.errCh <- eh.EventBusError{Err: fmt.Errorf("could not handle event (%s): %s", h.HandlerType(), err.Error()), Ctx: ctx, Event: event}:
		default:
		}
		return
	}

	consumer.MarkOffset(msg, "")
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType     eh.EventType           `bson:"event_type"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	data          eh.EventData           `bson:"-"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   eh.UUID                `bson:"_id"`
	Version       int                    `bson:"version"`
	Context       map[string]interface{} `bson:"context"`
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	evt
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.evt.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.evt.data
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.evt.Timestamp
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.evt.AggregateType
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event interface.
func (e event) AggregateID() eh.UUID {
	return e.evt.AggregateID
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.evt.Version
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.evt.EventType, e.evt.Version)
}

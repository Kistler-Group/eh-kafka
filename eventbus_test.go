/**
 * Copyright (c) 2018 KISTLER INSTRUMENTE AG, Winterthur, Switzerland
 *
 * @file eventbus_test.go
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
	"os"
	"testing"
	"time"

	sarama "github.com/Shopify/sarama"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/eventbus"
)

func TestEventBus(t *testing.T) {
	// Connect to localhost if not running inside docker
	broker := os.Getenv("KAFKA_EMULATOR_BOOTSTRAP_SERVER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topic := eh.NewUUID()

	config := sarama.NewConfig()
	config.Producer.Flush.MaxMessages = 1

	timeout := time.Second * 30

	bus1, err := NewEventBus([]string{broker}, config, timeout, func(eh.Event) string { return topic.String() }, func(eh.EventHandler) []string { return []string{topic.String()} })
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	bus2, err := NewEventBus([]string{broker}, config, timeout, func(eh.Event) string { return topic.String() }, func(eh.EventHandler) []string { return []string{topic.String()} })
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	eventbus.AcceptanceTest(t, bus1, bus2, timeout)

}

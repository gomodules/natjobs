// Copyright 2022-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nuid"
)

type (

	// Consumer contains methods for fetching/processing messages from a stream, as well as fetching consumer info
	Consumer interface {
		// Fetch is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and wait until either all messages are retrieved
		// or request times out.
		Fetch(batch int, opts ...FetchOpt) (MessageBatch, error)
		// FetchBytes is used to retrieve up to a provided bytes from the stream.
		// This method will always send a single request and wait until provided number of bytes is
		// exceeded or request times out.
		FetchBytes(maxBytes int, opts ...FetchOpt) (MessageBatch, error)
		// FetchNoWait is used to retrieve up to a provided number of messages from a stream.
		// This method will always send a single request and immediately return up to a provided number of messages.
		FetchNoWait(batch int) (MessageBatch, error)
		// Consume can be used to continuously receive messages and handle them with the provided callback function
		Consume(handler MessageHandler, opts ...PullConsumeOpt) (ConsumeContext, error)
		// Messages returns [MessagesContext], allowing continuously iterating over messages on a stream.
		Messages(opts ...PullMessagesOpt) (MessagesContext, error)
		// Next is used to retrieve the next message from the stream.
		// This method will block until the message is retrieved or timeout is reached.
		Next(opts ...FetchOpt) (Msg, error)

		// Info returns Consumer details
		Info(context.Context) (*ConsumerInfo, error)
		// CachedInfo returns [*ConsumerInfo] cached on a consumer struct
		CachedInfo() *ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string          `json:"stream_name"`
		Config *ConsumerConfig `json:"config"`
		Action string          `json:"action"`
	}
)

// Info returns [ConsumerInfo] for a given consumer
func (p *pullConsumer) Info(ctx context.Context) (*ConsumerInfo, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	infoSubject := apiSubj(p.jetStream.apiPrefix, fmt.Sprintf(apiConsumerInfoT, p.stream, p.name))
	var resp consumerInfoResponse

	if _, err := p.jetStream.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	p.info = resp.ConsumerInfo
	return resp.ConsumerInfo, nil
}

// CachedInfo returns [ConsumerInfo] fetched when initializing/updating a consumer
//
// NOTE: The returned object might not be up to date with the most recent updates on the server
// For up-to-date information, use [Info]
func (p *pullConsumer) CachedInfo() *ConsumerInfo {
	return p.info
}

func upsertConsumer(ctx context.Context, js *jetStream, stream string, cfg ConsumerConfig, action string) (Consumer, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	req := createConsumerRequest{
		Stream: stream,
		Config: &cfg,
		Action: action,
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	consumerName := cfg.Name
	if consumerName == "" {
		if cfg.Durable != "" {
			consumerName = cfg.Durable
		} else {
			consumerName = generateConsName()
		}
	}
	if err := validateConsumerName(consumerName); err != nil {
		return nil, err
	}

	var ccSubj string
	if cfg.FilterSubject != "" && len(cfg.FilterSubjects) == 0 {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateWithFilterSubjectT, stream, consumerName, cfg.FilterSubject))
	} else {
		ccSubj = apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerCreateT, stream, consumerName))
	}
	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, ccSubj, &resp, reqJSON); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeStreamNotFound {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	// check whether multiple filter subjects (if used) are reflected in the returned ConsumerInfo
	if len(cfg.FilterSubjects) != 0 && len(resp.Config.FilterSubjects) == 0 {
		return nil, ErrConsumerMultipleFilterSubjectsNotSupported
	}

	return &pullConsumer{
		jetStream:     js,
		stream:        stream,
		name:          resp.Name,
		durable:       cfg.Durable != "",
		info:          resp.ConsumerInfo,
		subscriptions: make(map[string]*pullSubscription),
	}, nil
}

const (
	consumerActionCreate         = "create"
	consumerActionUpdate         = "update"
	consumerActionCreateOrUpdate = ""
)

func generateConsName() string {
	name := nuid.Next()
	sha := sha256.New()
	sha.Write([]byte(name))
	b := sha.Sum(nil)
	for i := 0; i < 8; i++ {
		b[i] = rdigits[int(b[i]%base)]
	}
	return string(b[:8])
}

func getConsumer(ctx context.Context, js *jetStream, stream, name string) (Consumer, error) {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateConsumerName(name); err != nil {
		return nil, err
	}
	infoSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerInfoT, stream, name))

	var resp consumerInfoResponse

	if _, err := js.apiRequestJSON(ctx, infoSubject, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return nil, ErrConsumerNotFound
		}
		return nil, resp.Error
	}

	cons := &pullConsumer{
		jetStream:     js,
		stream:        stream,
		name:          name,
		durable:       resp.Config.Durable != "",
		info:          resp.ConsumerInfo,
		subscriptions: make(map[string]*pullSubscription, 0),
	}

	return cons, nil
}

func deleteConsumer(ctx context.Context, js *jetStream, stream, consumer string) error {
	ctx, cancel := wrapContextWithoutDeadline(ctx)
	if cancel != nil {
		defer cancel()
	}
	if err := validateConsumerName(consumer); err != nil {
		return err
	}
	deleteSubject := apiSubj(js.apiPrefix, fmt.Sprintf(apiConsumerDeleteT, stream, consumer))

	var resp consumerDeleteResponse

	if _, err := js.apiRequestJSON(ctx, deleteSubject, &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		if resp.Error.ErrorCode == JSErrCodeConsumerNotFound {
			return ErrConsumerNotFound
		}
		return resp.Error
	}
	return nil
}

func validateConsumerName(dur string) error {
	if strings.Contains(dur, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidConsumerName, dur)
	}
	return nil
}

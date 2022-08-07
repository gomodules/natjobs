/*
Copyright AppsCode Inc. and Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tasks

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-logr/logr/funcr"
)

type TaskType string

var (
	reg       = map[TaskType]TaskDef{}
	m         sync.RWMutex
	ctxType   = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType = reflect.TypeOf((*error)(nil)).Elem()
)

type TaskDef struct {
	taskType       TaskType
	dataType       reflect.Type
	fn             interface{}
	respLoggerOpts *funcr.Options
}

func (t TaskDef) TaskType() TaskType {
	return t.taskType
}

func (t TaskDef) NewObj() any {
	return reflect.New(t.dataType).Interface()
}

func (t TaskDef) Fn() interface{} {
	return t.fn
}

func (t TaskDef) RespLoggerOpts() *funcr.Options {
	return t.respLoggerOpts
}

func Register(t TaskType, fn interface{}, opts ...funcr.Options) error {
	m.Lock()
	defer m.Unlock()

	typ := reflect.TypeOf(fn)
	if typ.Kind() != reflect.Func {
		return fmt.Errorf("fn %s must be a function, found %s", typ, typ.Kind())
	}

	switch typ.NumIn() {
	case 2:
		etyp := typ.In(0)
		if !etyp.Implements(ctxType) {
			return fmt.Errorf("fn %s's first argument must be context.Context", typ)
		}
	default:
		return fmt.Errorf("fn %s is expected take 2 arguments(ctx context.Context, in T)", typ)
	}

	switch typ.NumOut() {
	case 1:
		etyp := typ.Out(0)
		if !etyp.Implements(errorType) {
			if reflect.New(etyp).Type().Implements(errorType) {
				return fmt.Errorf("fn %s return type should be *%s to be considered an error", typ, etyp.Name())
			}
			return fmt.Errorf("fn %s return type must be an error", typ)
		}
	default:
		return fmt.Errorf("fn %s must only return an error", typ)
	}

	var def TaskDef
	def.taskType = t
	def.fn = fn
	def.dataType = typ.In(1)
	if len(opts) > 0 {
		def.respLoggerOpts = &opts[0]
	}
	reg[t] = def

	return nil
}

func MustRegister(t TaskType, fn interface{}) {
	if err := Register(t, fn); err != nil {
		panic(err)
	}
}

func Get(t TaskType) (TaskDef, bool) {
	m.RLock()
	defer m.RUnlock()

	def, ok := reg[t]
	return def, ok
}

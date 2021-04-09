// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package logger

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"

	"github.com/apache/skywalking-banyandb/pkg/config"
)

func TestInitLogger(t *testing.T) {
	type args struct {
		cfg config.Logging
	}
	type want struct {
		isDev bool
		level zapcore.Level
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{config.Logging{Env: "prod", Level: "info"}},
			want: want{level: zapcore.InfoLevel},
		},
		{
			name: "empty config",
			args: args{config.Logging{}},
			want: want{level: zapcore.InfoLevel},
		},
		{
			name: "development mode",
			args: args{config.Logging{Env: "dev"}},
			want: want{isDev: true, level: zapcore.InfoLevel},
		},
		{
			name: "debug level",
			args: args{config.Logging{Level: "debug"}},
			want: want{level: zapcore.DebugLevel},
		},
		{
			name: "invalid env",
			args: args{config.Logging{Env: "invalid"}},
			want: want{level: zapcore.InfoLevel},
		},
		{
			name:    "invalid level",
			args:    args{config.Logging{Level: "invalid"}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			var logger *Logger
			if logger, err = getLogger(tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("InitLogger() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				assert.NotNil(t, logger)
				assert.NotNil(t, logger.logger)
				assert.NotEmpty(t, logger.module)
				assert.Equal(t, tt.want.isDev, reflect.ValueOf(*logger.logger).FieldByName("development").Bool())
				assert.NotNil(t, logger.logger.Check(tt.want.level, "foo"))
			}
		})
	}
}

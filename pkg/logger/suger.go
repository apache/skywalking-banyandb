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

// Panicf logs a message at the panic level.
// It panics after logging the message.
// It uses sprintf-style formatting for its message.
func Panicf(f string, v ...interface{}) {
	GetLogger().Panic().Msgf(f, v...)
}

// Errorf logs a message at the error level.
// It uses sprintf-style formatting for its message.
func Errorf(f string, v ...interface{}) {
	GetLogger().Error().Msgf(f, v...)
}

// Warningf logs a message at the warning level.
// It uses sprintf-style formatting for its message.
func Warningf(f string, v ...interface{}) {
	GetLogger().Warn().Msgf(f, v...)
}

// Infof logs a message at the info level.
// It uses sprintf-style formatting for its message.
func Infof(f string, v ...interface{}) {
	GetLogger().Info().Msgf(f, v...)
}

// Debugf logs a message at the debug level.
// It uses sprintf-style formatting for its message.
func Debugf(f string, v ...interface{}) {
	GetLogger().Debug().Msgf(f, v...)
}

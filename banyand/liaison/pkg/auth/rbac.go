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

package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"
)

// ErrInvalidPolicy is returned when security configuration cannot be compiled into a
// snapshot: an unknown permission, a binding to an undeclared role, a duplicate role or
// user name, or malformed YAML.
var ErrInvalidPolicy = errors.New("invalid security policy")

// Permission is a capability a role may hold. The vocabulary is closed: a permission
// outside this set makes the whole configuration invalid.
type Permission string

// The permission vocabulary. Cluster permissions are decided from the security snapshot
// alone. Schema and data permissions additionally need group scope, which the liaison
// resolves in a later release; a method classified with one of them fails closed.
const (
	PermissionClusterRead  Permission = "cluster:read"
	PermissionClusterAdmin Permission = "cluster:admin"
	PermissionSchemaRead   Permission = "schema:read"
	PermissionSchemaWrite  Permission = "schema:write"
	PermissionDataRead     Permission = "data:read"
	PermissionDataWrite    Permission = "data:write"
)

// Permissions returns every permission the vocabulary defines, in a fixed order.
func Permissions() []Permission {
	return []Permission{
		PermissionClusterRead,
		PermissionClusterAdmin,
		PermissionSchemaRead,
		PermissionSchemaWrite,
		PermissionDataRead,
		PermissionDataWrite,
	}
}

// Role is a flat, named set of permissions. Roles do not nest or inherit.
type Role struct {
	Name        string       `json:"name"        yaml:"name"`
	Permissions []Permission `json:"permissions" yaml:"permissions"`
}

// Binding grants one user one role. A user named in `users` but in no binding is
// authenticated and holds no permission.
type Binding struct {
	Username string `json:"username" yaml:"username"`
	Role     string `json:"role"     yaml:"role"`
}

// RBACSection is the optional `rbac` block of the security configuration file. Omitting
// the block, and setting Enabled to false, both leave role-based access control off and
// the file behaving exactly as a users-only file does.
type RBACSection struct {
	Roles    []Role    `json:"roles"    yaml:"roles"`
	Bindings []Binding `json:"bindings" yaml:"bindings"`
	Enabled  bool      `json:"enabled"  yaml:"enabled"`
}

// Principal is the trusted identity of a caller whose credentials a Snapshot verified.
// Only Snapshot.Authenticate mints a non-zero Principal, so an identity a caller supplied
// in gRPC metadata or an HTTP header can never become one.
type Principal struct {
	username string
}

// Username returns the verified user name of the principal.
func (p Principal) Username() string {
	return p.username
}

// IsZero reports whether p is the empty principal, which no credential check produced.
func (p Principal) IsZero() bool {
	return p.username == ""
}

// Snapshot is an immutable view of the security configuration at one revision. A caller
// holding a Snapshot observes the same credentials and grants for as long as it holds it,
// regardless of any reload that happens meanwhile.
type Snapshot interface {
	// Revision returns the revision this snapshot was compiled at. Revisions increase by
	// one for every configuration a Reloader accepts, and do not move for one it rejects.
	Revision() uint64
	// RBACEnabled reports whether the configuration turned role-based access control on.
	RBACEnabled() bool
	// Authenticate verifies username and password in constant time against the snapshot's
	// credentials and returns the trusted principal for them.
	Authenticate(username, password string) (Principal, bool)
	// Allows reports whether principal holds perm under this snapshot's compiled grants.
	// It reports false for the zero principal and for any principal the snapshot does not
	// know, so an unbound user holds nothing.
	Allows(principal Principal, perm Permission) bool
}

type credential struct {
	username       string
	usernameDigest [sha256.Size]byte
	passwordDigest [sha256.Size]byte
}

type compiledSnapshot struct {
	grants      map[string]map[Permission]struct{}
	credentials []credential
	revision    uint64
	rbacEnabled bool
}

var initialSnapshot = &compiledSnapshot{
	credentials: []credential{},
	grants:      make(map[string]map[Permission]struct{}),
}

// CompileSnapshot parses raw security configuration bytes and compiles them into an
// immutable snapshot stamped with the given revision. A file carrying only a `users`
// list compiles to a snapshot whose credentials work and whose RBACEnabled reports false.
// Configuration that cannot be trusted is rejected with an error wrapping
// ErrInvalidPolicy rather than compiled into a partial grant set.
func CompileSnapshot(revision uint64, raw []byte) (Snapshot, error) {
	snapshot, compileErr := compileSnapshot(revision, raw)
	if compileErr != nil {
		return nil, compileErr
	}
	return snapshot, nil
}

func compileSnapshot(revision uint64, raw []byte) (*compiledSnapshot, error) {
	var configuration struct {
		Users []User      `yaml:"users"`
		RBAC  RBACSection `yaml:"rbac"`
	}
	if unmarshalErr := yaml.Unmarshal(raw, &configuration); unmarshalErr != nil {
		return nil, invalidPolicyError("decode security configuration", unmarshalErr)
	}

	credentials, users, usersErr := compileCredentials(configuration.Users)
	if usersErr != nil {
		return nil, usersErr
	}
	snapshot := &compiledSnapshot{
		revision:    revision,
		rbacEnabled: configuration.RBAC.Enabled,
		credentials: credentials,
		grants:      make(map[string]map[Permission]struct{}),
	}
	if !configuration.RBAC.Enabled {
		return snapshot, nil
	}

	roles, rolesErr := compileRoles(configuration.RBAC.Roles)
	if rolesErr != nil {
		return nil, rolesErr
	}
	for _, binding := range configuration.RBAC.Bindings {
		username := strings.TrimSpace(binding.Username)
		roleName := strings.TrimSpace(binding.Role)
		if _, exists := users[username]; !exists {
			return nil, invalidPolicyError(fmt.Sprintf("binding references undeclared user %q", username), nil)
		}
		permissions, exists := roles[roleName]
		if !exists {
			return nil, invalidPolicyError(fmt.Sprintf("binding references undeclared role %q", roleName), nil)
		}
		grants, exists := snapshot.grants[username]
		if !exists {
			grants = make(map[Permission]struct{}, len(permissions))
			snapshot.grants[username] = grants
		}
		for permission := range permissions {
			grants[permission] = struct{}{}
		}
	}
	return snapshot, nil
}

func compileCredentials(users []User) ([]credential, map[string]struct{}, error) {
	credentials := make([]credential, 0, len(users))
	knownUsers := make(map[string]struct{}, len(users))
	for _, user := range users {
		username := strings.TrimSpace(user.Username)
		if username == "" {
			return nil, nil, invalidPolicyError("user has an empty username", nil)
		}
		if _, exists := knownUsers[username]; exists {
			return nil, nil, invalidPolicyError(fmt.Sprintf("duplicate user %q", username), nil)
		}
		knownUsers[username] = struct{}{}
		password := strings.TrimSpace(user.Password)
		credentials = append(credentials, credential{
			username:       username,
			usernameDigest: sha256.Sum256([]byte(username)),
			passwordDigest: sha256.Sum256([]byte(password)),
		})
	}
	return credentials, knownUsers, nil
}

func compileRoles(roles []Role) (map[string]map[Permission]struct{}, error) {
	compiledRoles := make(map[string]map[Permission]struct{}, len(roles))
	for _, role := range roles {
		roleName := strings.TrimSpace(role.Name)
		if roleName == "" {
			return nil, invalidPolicyError("role has an empty name", nil)
		}
		if _, exists := compiledRoles[roleName]; exists {
			return nil, invalidPolicyError(fmt.Sprintf("duplicate role %q", roleName), nil)
		}
		permissions := make(map[Permission]struct{}, len(role.Permissions))
		for _, permission := range role.Permissions {
			if !isKnownPermission(permission) {
				return nil, invalidPolicyError(fmt.Sprintf("role %q has unknown permission %q", roleName, permission), nil)
			}
			permissions[permission] = struct{}{}
		}
		compiledRoles[roleName] = permissions
	}
	return compiledRoles, nil
}

func invalidPolicyError(message string, cause error) error {
	if cause != nil {
		return fmt.Errorf("%w: %s: %w", ErrInvalidPolicy, message, cause)
	}
	return fmt.Errorf("%w: %s", ErrInvalidPolicy, message)
}

func isKnownPermission(permission Permission) bool {
	switch permission {
	case PermissionClusterRead, PermissionClusterAdmin, PermissionSchemaRead, PermissionSchemaWrite, PermissionDataRead, PermissionDataWrite:
		return true
	default:
		return false
	}
}

func (s *compiledSnapshot) Revision() uint64 {
	return s.revision
}

func (s *compiledSnapshot) RBACEnabled() bool {
	return s.rbacEnabled
}

func (s *compiledSnapshot) Authenticate(username, password string) (Principal, bool) {
	usernameDigest := sha256.Sum256([]byte(strings.TrimSpace(username)))
	passwordDigest := sha256.Sum256([]byte(strings.TrimSpace(password)))
	matchedUsername := ""
	matched := 0
	for _, credential := range s.credentials {
		usernameMatch := subtle.ConstantTimeCompare(usernameDigest[:], credential.usernameDigest[:])
		passwordMatch := subtle.ConstantTimeCompare(passwordDigest[:], credential.passwordDigest[:])
		if usernameMatch&passwordMatch == 1 {
			matchedUsername = credential.username
			matched = 1
		}
	}
	if matched != 1 {
		return Principal{}, false
	}
	return Principal{username: matchedUsername}, true
}

func (s *compiledSnapshot) Allows(principal Principal, permission Permission) bool {
	if !s.rbacEnabled || principal.IsZero() {
		return false
	}
	permissions, exists := s.grants[principal.username]
	if !exists {
		return false
	}
	_, allowed := permissions[permission]
	return allowed
}

// CurrentSnapshot returns the security snapshot in force. It never returns nil: before any
// configuration is loaded it returns an empty snapshot at revision 0 with RBAC disabled and
// no credentials. Successive calls return the snapshot published by the most recent
// accepted reload, and a rejected reload leaves the previous one in force.
func (ar *Reloader) CurrentSnapshot() Snapshot {
	if ar == nil {
		return initialSnapshot
	}
	snapshot := ar.snapshot.Load()
	if snapshot == nil {
		return initialSnapshot
	}
	return snapshot
}

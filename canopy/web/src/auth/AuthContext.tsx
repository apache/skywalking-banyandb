/*
 * Licensed to Apache Software Foundation (ASF) under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Apache Software Foundation (ASF) licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, { createContext, useContext, useEffect, useState } from 'react';

export interface Session {
  readonly user: string;
  readonly role: 'admin' | 'readonly';
  readonly banyanVersion: string | null;
}

interface AuthState {
  readonly session: Session | null;
  readonly loading: boolean;
  readonly setSession: (s: Session | null) => void;
}

const AuthContext = createContext<AuthState>({
  session: null,
  loading: true,
  setSession: () => undefined,
});

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [session, setSession] = useState<Session | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/auth/session')
      .then(res => (res.ok ? res.json() as Promise<Session> : null))
      .then(s => { setSession(s); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  return (
    <AuthContext.Provider value={{ session, loading, setSession }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}

export function useCanWrite() {
  const { session } = useAuth();
  return session?.role === 'admin';
}

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

import React, { useState, useEffect } from 'react';

import { CanopyMark } from '../components/CanopyMark.js';
import { useAuth } from '../auth/AuthContext.js';
import { IconShield, IconViewer } from '../components/icons.js';

type Role = 'admin' | 'readonly';
type Status = 'idle' | 'connecting' | 'connected' | 'failed';

interface FieldProps {
  label: string;
  required?: boolean;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  mono?: boolean;
  error?: string;
  eye?: boolean;
  eyeOn?: boolean;
  onEye?: () => void;
  onSubmit?: () => void;
}

function EyeIcon({ off }: { off: boolean }) {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
      {off ? (
        <>
          <path d="M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 10 8 10 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24" />
          <path d="M6.61 6.61A18.5 18.5 0 0 0 2 12s3 8 10 8a9.12 9.12 0 0 0 5.39-1.61" />
          <path d="m2 2 20 20" />
        </>
      ) : (
        <>
          <path d="M2 12s3-8 10-8 10 8 10 8-3 8-10 8-10-8-10-8Z" />
          <circle cx="12" cy="12" r="3" />
        </>
      )}
    </svg>
  );
}


function Field({ label, required, value, onChange, type = 'text', placeholder, mono, error, eye, eyeOn, onEye, onSubmit }: FieldProps) {
  const fieldId = 'f-' + label.toLowerCase().replace(/\s+/g, '-');
  return (
    <div className={'f-field' + (error ? ' has-error' : '')}>
      <label className="f-label" htmlFor={fieldId}>
        {label}
        {required && <span className="f-req">*</span>}
      </label>
      <div className="f-inputwrap">
        <input
          id={fieldId}
          className={'f-input' + (mono ? ' mono' : '') + (eye ? ' has-eye' : '')}
          type={type}
          value={value}
          placeholder={placeholder}
          spellCheck={false}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter' && onSubmit) onSubmit(); }}
        />
        {eye && (
          <button type="button" className="f-eye" tabIndex={-1} onClick={onEye} aria-label="Toggle visibility">
            <EyeIcon off={!eyeOn} />
          </button>
        )}
      </div>
      {error && <div className="f-error">{error}</div>}
    </div>
  );
}

const ROLES: Record<Role, { label: string; desc: string; Icon: React.FC<{ size?: number }> }> = {
  admin: { label: 'Administrator', desc: 'Full control', Icon: IconShield },
  readonly: { label: 'Read-only', desc: 'View only', Icon: IconViewer },
};

export function LoginPage() {
  const { setSession } = useAuth();
  const [role, setRole] = useState<Role>('admin');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPass, setShowPass] = useState(false);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [status, setStatus] = useState<Status>('idle');
  const [errorMsg, setErrorMsg] = useState('');
  const [banyanVersion, setBanyanVersion] = useState<string | null>(null);
  const [banyanReachable, setBanyanReachable] = useState<boolean | null>(null);

  // Probe the BFF's configured BanyanDB upstream on mount so the badge can
  // show the version and reachability before the user has typed anything.
  // The BanyanDB target itself is configured server-side (BANYANDB_TARGET)
  // and is no longer a per-session input.
  useEffect(() => {
    fetch('/api/meta')
      .then(r => r.ok ? r.json() as Promise<{ banyanVersion: string | null; reachable?: boolean }> : null)
      .then(d => {
        if (!d) { setBanyanReachable(false); setBanyanVersion(null); return; }
        if (typeof d.reachable === 'boolean') setBanyanReachable(d.reachable);
        // Only show a version for the upstream we just probed. A failed probe
        // clears the version so the badge never shows a misleading
        // "BanyanDB · v0.10" while reachable=false.
        setBanyanVersion(d.reachable ? (d.banyanVersion ?? null) : null);
      })
      .catch(() => { setBanyanReachable(false); setBanyanVersion(null); });
  }, []);

  const clearFieldError = (key: string) => {
    if (errors[key]) setErrors((prev) => { const next = { ...prev }; delete next[key]; return next; });
    if (status === 'failed') setStatus('idle');
  };

  const pickRole = (nextRole: Role) => {
    setRole(nextRole);
    setUsername('');
    setPassword('');
    setErrors({});
    if (status === 'failed') setStatus('idle');
  };

  const submit = async () => {
    const fieldErrors: Record<string, string> = {};
    if (!username.trim()) fieldErrors.username = 'Username is required';
    if (!password) fieldErrors.password = 'Password is required';
    setErrors(fieldErrors);
    if (Object.keys(fieldErrors).length > 0) return;

    // Pre-flight: refuse to log in when BanyanDB is unreachable. Without
    // this, the user gets a successful login, the sidebar flips to
    // "Connected", but every /api/* call returns 502 — a confusing trap.
    // We re-probe /api/meta here (the same endpoint that drives the badge)
    // so the result is authoritative even if the mount-time probe is stale.
    let preflightReachable = banyanReachable;
    try {
      const probe = await fetch('/api/meta');
      if (probe.ok) {
        const meta = await probe.json() as { reachable?: boolean };
        preflightReachable = typeof meta.reachable === 'boolean' ? meta.reachable : preflightReachable;
      }
    } catch {
      // If the probe itself fails, fall through to the server's own check.
    }
    if (preflightReachable === false) {
      setErrorMsg('Cannot reach the BanyanDB upstream. Check BANYANDB_TARGET on the BFF and the BanyanDB process.');
      setStatus('failed');
      return;
    }

    setStatus('connecting');
    setErrorMsg('');
    try {
      const res = await fetch('/auth/login', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username: username.trim(), password }),
      });
      if (res.ok) {
        const data = (await res.json()) as { user: string; role: 'admin' | 'readonly'; banyanVersion: string | null };
        setStatus('connected');
        setSession({ user: data.user, role: data.role, banyanVersion: data.banyanVersion ?? null });
      } else {
        const body = await res.json().catch(() => ({})) as { message?: string };
        setErrorMsg(body.message ?? 'Authentication failed — check your credentials.');
        setStatus('failed');
      }
    } catch {
      setErrorMsg('Cannot reach the server — check your network connection.');
      setStatus('failed');
    }
  };

  const connecting = status === 'connecting';
  const connected = status === 'connected';
  // Gate the form on BanyanDB reachability — refuse to log in against an
  // unreachable upstream so the user never sees the "Connected but every
  // API fails" trap. banyanReachable === null means the probe is still in
  // flight; we let the user try (the pre-flight in submit() will catch it).
  const upstreamDown = banyanReachable === false;

  return (
    <div className="lf">
      <div className="lf-grid" />
      <div className="lf-card">
        <div className="lf-brand">
          <CanopyMark size={76} glow />
          <h1 className="lf-wordmark">Canopy</h1>
          <p className="lf-eyebrow">BanyanDB Console</p>
          <div className="lf-meta">
            <span>{banyanVersion ? `BanyanDB · v${banyanVersion}` : 'BanyanDB'}</span>
            <span className="lf-sep" />
            {banyanReachable === false ? (
              <span className="lf-warn" role="alert">
                <svg className="lf-warn-ico" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                  <path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z" />
                  <line x1="12" y1="9" x2="12" y2="13" />
                  <line x1="12" y1="17" x2="12.01" y2="17" />
                </svg>
                <span className="lf-warn-label">Server unreachable</span>
              </span>
            ) : (
              <span className="lf-live">HTTP ready</span>
            )}
          </div>
          {banyanReachable === false && (
            <p className="lf-warn-hint" role="status">
              BanyanDB upstream is unreachable.
            </p>
          )}
        </div>
        <div className="lf-formwrap">
          <form
            className="lf-form"
            onSubmit={(e) => { e.preventDefault(); void submit(); }}
          >
            <div className="f-field lf-roles">
              <label className="f-label">Sign in as</label>
              <div className="lf-seg" role="radiogroup" aria-label="Role">
                {(Object.keys(ROLES) as Role[]).map((r) => {
                  const def = ROLES[r];
                  return (
                    <button
                      type="button"
                      key={r}
                      className={'lf-seg-btn' + (role === r ? ' on' : '')}
                      role="radio"
                      aria-checked={role === r}
                      onClick={() => pickRole(r)}
                    >
                      <span className="seg-ico"><def.Icon size={15} /></span>
                      <span className="seg-txt">
                        <span className="seg-t">{def.label}</span>
                        <span className="seg-d">{def.desc}</span>
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>
            <Field
              label="Username"
              required
              value={username}
              onChange={(v) => { setUsername(v); clearFieldError('username'); }}
              placeholder="username"
              error={errors.username}
              onSubmit={() => void submit()}
            />
            <Field
              label="Password"
              required
              value={password}
              onChange={(v) => { setPassword(v); clearFieldError('password'); }}
              type={showPass ? 'text' : 'password'}
              placeholder="••••••••"
              error={errors.password}
              eye
              eyeOn={showPass}
              onEye={() => setShowPass((p) => !p)}
              onSubmit={() => void submit()}
            />
            {status === 'failed' && (
              <div className="lf-banner err" role="alert">
                <span className="lf-dot" />
                <span>{errorMsg}</span>
              </div>
            )}
            {connected && (
              <div className="lf-banner ok" role="status">
                <span className="lf-dot" />
                <span>Connected — opening console…</span>
              </div>
            )}
            <button
              type="submit"
              className={'btn btn-primary' + (connected ? ' is-ok' : '') + (upstreamDown ? ' is-disabled-by-upstream' : '')}
              disabled={connecting || connected || upstreamDown}
            >
              {connecting ? (
                <><span className="spin" />Connecting…</>
              ) : connected ? (
                'Connected'
              ) : upstreamDown ? (
                'Upstream unreachable'
              ) : (
                'Connect'
              )}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}
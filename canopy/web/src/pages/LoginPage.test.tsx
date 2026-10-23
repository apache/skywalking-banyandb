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

import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';

import { AuthProvider } from '../auth/AuthContext.js';
import { LoginPage } from './LoginPage.js';

function Wrapper({ children }: { children: React.ReactNode }) {
  return (
    <MemoryRouter>
      <AuthProvider>{children}</AuthProvider>
    </MemoryRouter>
  );
}

function renderLogin() {
  return render(<LoginPage />, { wrapper: Wrapper });
}

// The credential form appears once /api/meta has confirmed the local backend.
async function renderLocalLogin() {
  const view = renderLogin();
  await screen.findByLabelText(/Username/i);
  return view;
}

describe('LoginPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Default: /auth/session returns 401 (unauthenticated)
    globalThis.fetch = vi.fn((url: RequestInfo | URL) => {
      const urlStr = String(url);
      if (urlStr.includes('/auth/session')) {
        return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
      }
      return Promise.resolve(new Response('{}', { status: 200 }));
    }) as typeof fetch;
  });

  it('renders username, password, and role fields', async () => {
    await renderLocalLogin();
    expect(screen.getByLabelText(/Username/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Password/i)).toBeInTheDocument();
    // Endpoint is no longer a per-session input — the BFF owns the BanyanDB target.
    expect(screen.queryByLabelText(/Endpoint/i)).not.toBeInTheDocument();
  });

  it('renders the Connect button', async () => {
    await renderLocalLogin();
    expect(screen.getByRole('button', { name: /Connect/i })).toBeInTheDocument();
  });

  it('renders the role picker with admin and readonly options', async () => {
    await renderLocalLogin();
    expect(screen.getByRole('radio', { name: /Administrator/i })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Read-only/i })).toBeInTheDocument();
  });

  it('shows validation errors when submitting empty form', async () => {
    await renderLocalLogin();
    const usernameInput = screen.getByLabelText(/Username/i);
    fireEvent.change(usernameInput, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: /Connect/i }));
    await waitFor(() => {
      expect(screen.getByText(/Username is required/i)).toBeInTheDocument();
    });
  });

  it('calls /auth/login on submit with credentials', async () => {
    const mockFetch = vi.fn((url: RequestInfo | URL, _init?: RequestInit) => {
      const urlStr = String(url);
      if (urlStr.includes('/auth/session')) {
        return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
      }
      if (urlStr.includes('/auth/login')) {
        return Promise.resolve(new Response(JSON.stringify({ user: 'admin', role: 'admin', banyanVersion: '0.10' }), { status: 200 }));
      }
      return Promise.resolve(new Response('{}', { status: 200 }));
    });
    globalThis.fetch = mockFetch as unknown as typeof fetch;

    const user = userEvent.setup();
    await renderLocalLogin();

    const usernameInput = screen.getByLabelText(/Username/i);
    const passwordInput = screen.getByLabelText(/Password/i);
    await user.type(usernameInput, 'admin');
    await user.type(passwordInput, 'admin');

    await user.click(screen.getByRole('button', { name: /Connect/i }));

    await waitFor(() => {
      const loginCalls = mockFetch.mock.calls.filter(([url]) => String(url).includes('/auth/login'));
      expect(loginCalls.length).toBeGreaterThan(0);
      const body = JSON.parse(loginCalls[0][1]?.body as string);
      expect(body.username).toBe('admin');
      expect(body.password).toBe('admin');
    });
  });

  it('shows error banner on 401 response', async () => {
    globalThis.fetch = vi.fn((url: RequestInfo | URL) => {
      const urlStr = String(url);
      if (urlStr.includes('/auth/session')) {
        return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
      }
      if (urlStr.includes('/auth/login')) {
        return Promise.resolve(new Response(JSON.stringify({ message: 'Invalid username or password' }), { status: 401 }));
      }
      return Promise.resolve(new Response('{}', { status: 200 }));
    }) as typeof fetch;

    const user = userEvent.setup();
    await renderLocalLogin();

    await user.type(screen.getByLabelText(/Username/i), 'admin');
    await user.type(screen.getByLabelText(/Password/i), 'wrongpass');
    await user.click(screen.getByRole('button', { name: /Connect/i }));

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeInTheDocument();
      expect(screen.getByText(/Invalid username or password/i)).toBeInTheDocument();
    });
  });

  it('toggles password visibility when eye button clicked', async () => {
    await renderLocalLogin();
    const passwordInput = screen.getByLabelText(/Password/i);
    expect(passwordInput).toHaveAttribute('type', 'password');

    const eyeBtn = screen.getByLabelText(/Toggle visibility/i);
    fireEvent.click(eyeBtn);
    expect(passwordInput).toHaveAttribute('type', 'text');

    fireEvent.click(eyeBtn);
    expect(passwordInput).toHaveAttribute('type', 'password');
  });

  describe('with the sso backend', () => {
    function mockMeta(auth: object, reachable = true) {
      globalThis.fetch = vi.fn((url: RequestInfo | URL) => {
        const urlStr = String(url);
        if (urlStr.includes('/auth/session')) {
          return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
        }
        if (urlStr.includes('/api/meta')) {
          return Promise.resolve(new Response(JSON.stringify({ banyanVersion: '0.12.0', reachable, auth }), { status: 200 }));
        }
        return Promise.resolve(new Response('{}', { status: 200 }));
      }) as typeof fetch;
    }

    const providers = [{ id: 'google', name: 'Google', type: 'oidc' }, { id: 'github', name: 'GitHub', type: 'oauth2' }];

    it('shows no credential form until the server has answered, and offers a retry when it fails', async () => {
      globalThis.fetch = vi.fn((url: RequestInfo | URL) => {
        if (String(url).includes('/auth/session')) {
          return Promise.resolve(new Response('{}', { status: 401 }));
        }
        return Promise.reject(new Error('network down'));
      }) as typeof fetch;
      renderLogin();

      expect(screen.queryByLabelText(/Username/i)).not.toBeInTheDocument();
      expect(await screen.findByRole('button', { name: 'Retry' })).toBeInTheDocument();
      expect(screen.queryByLabelText(/Username/i)).not.toBeInTheDocument();

      mockMeta({ backend: 'sso', providers, ssoHealthy: true, breakGlass: false });
      fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
      expect(await screen.findByRole('link', { name: 'Sign in with Google' })).toBeInTheDocument();
    });

    it('shows a button per provider and no password form while SSO is healthy', async () => {
      mockMeta({ backend: 'sso', providers, ssoHealthy: true, breakGlass: true });
      renderLogin();

      const google = await screen.findByRole('link', { name: 'Sign in with Google' });
      expect(google).toHaveAttribute('href', expect.stringContaining('/auth/sso/google/start'));
      expect(screen.getByRole('link', { name: 'Sign in with GitHub' })).toBeInTheDocument();
      expect(screen.queryByLabelText(/Username/i)).not.toBeInTheDocument();
    });

    it('does not offer SSO sign-in while the BanyanDB upstream is unreachable', async () => {
      mockMeta({ backend: 'sso', providers, ssoHealthy: true, breakGlass: false }, false);
      renderLogin();

      const buttons = await screen.findAllByRole('button', { name: 'Upstream unreachable' });
      expect(buttons).toHaveLength(providers.length);
      buttons.forEach(button => expect(button).toBeDisabled());
      expect(screen.queryByRole('link', { name: /Sign in with/ })).not.toBeInTheDocument();
    });

    it('shows the break-glass form only when SSO is unhealthy and break-glass is enabled', async () => {
      mockMeta({ backend: 'sso', providers, ssoHealthy: false, breakGlass: true });
      renderLogin();

      expect(await screen.findByLabelText(/Username/i)).toBeInTheDocument();
      expect(screen.getByLabelText(/Password/i)).toBeInTheDocument();
    });

    it('hides the password form when SSO is unhealthy but break-glass is off', async () => {
      mockMeta({ backend: 'sso', providers, ssoHealthy: false, breakGlass: false });
      renderLogin();

      await screen.findByRole('link', { name: 'Sign in with Google' });
      expect(screen.queryByLabelText(/Username/i)).not.toBeInTheDocument();
    });
  });
});

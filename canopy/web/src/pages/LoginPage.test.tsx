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
import { MemoryRouter } from 'react-router-dom';

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

describe('LoginPage', () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // Default: /auth/session returns 401 (unauthenticated)
    global.fetch = vi.fn((url: RequestInfo | URL) => {
      const urlStr = String(url);
      if (urlStr.includes('/auth/session')) {
        return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
      }
      return Promise.resolve(new Response('{}', { status: 200 }));
    }) as typeof fetch;
  });

  it('renders endpoint, username, and password fields', () => {
    renderLogin();
    expect(screen.getByLabelText(/Endpoint/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Username/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/Password/i)).toBeInTheDocument();
  });

  it('renders the Connect button', () => {
    renderLogin();
    expect(screen.getByRole('button', { name: /Connect/i })).toBeInTheDocument();
  });

  it('renders the role picker with admin and readonly options', () => {
    renderLogin();
    expect(screen.getByRole('radio', { name: /Administrator/i })).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: /Read-only/i })).toBeInTheDocument();
  });

  it('shows validation errors when submitting empty form', async () => {
    renderLogin();
    const endpointInput = screen.getByLabelText(/Endpoint/i);
    fireEvent.change(endpointInput, { target: { value: '' } });
    fireEvent.click(screen.getByRole('button', { name: /Connect/i }));
    await waitFor(() => {
      expect(screen.getByText(/Endpoint is required/i)).toBeInTheDocument();
    });
  });

  it('calls /auth/login on submit with credentials', async () => {
    const mockFetch = vi.fn((url: RequestInfo | URL, _init?: RequestInit) => {
      const urlStr = String(url);
      if (urlStr.includes('/auth/session')) {
        return Promise.resolve(new Response(JSON.stringify({ error: 'unauthenticated' }), { status: 401 }));
      }
      if (urlStr.includes('/auth/login')) {
        return Promise.resolve(new Response(JSON.stringify({ user: 'admin', role: 'admin', endpoint: 'http://localhost:17913' }), { status: 200 }));
      }
      return Promise.resolve(new Response('{}', { status: 200 }));
    });
    global.fetch = mockFetch as unknown as typeof fetch;

    const user = userEvent.setup();
    renderLogin();

    const passwordInput = screen.getByLabelText(/Password/i);
    await user.clear(passwordInput);
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
    global.fetch = vi.fn((url: RequestInfo | URL) => {
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
    renderLogin();

    await user.type(screen.getByLabelText(/Password/i), 'wrongpass');
    await user.click(screen.getByRole('button', { name: /Connect/i }));

    await waitFor(() => {
      expect(screen.getByRole('alert')).toBeInTheDocument();
      expect(screen.getByText(/Invalid username or password/i)).toBeInTheDocument();
    });
  });

  it('toggles password visibility when eye button clicked', async () => {
    renderLogin();
    const passwordInput = screen.getByLabelText(/Password/i);
    expect(passwordInput).toHaveAttribute('type', 'password');

    const eyeBtn = screen.getByLabelText(/Toggle visibility/i);
    fireEvent.click(eyeBtn);
    expect(passwordInput).toHaveAttribute('type', 'text');

    fireEvent.click(eyeBtn);
    expect(passwordInput).toHaveAttribute('type', 'password');
  });
});

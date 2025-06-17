# ui

This template should help get you started developing with Vue 3 in Vite.

## Recommended IDE Setup

VSCode + Volar (and disable Vetur) + TypeScript Vue Plugin (Volar).

## Setting up Environment Variables

First, you need to copy `.env.example` as `.env.development.local`. The environment variables explanation is as follows:

1. `VITE_API_PROXY` is to enable proxy to a API backend.
2. `VITE_MONITOR_PROXY` is to enable proxy to a monintor backend.

Example `.env.development.local` is as the following:

```
VITE_API_PROXY=http://127.0.0.1:17913
VITE_MONITOR_PROXY=http://127.0.0.1:2121
```

## Project Setup

```sh
npm install
```

### Compile and Hot-Reload for Development

```sh
npm run dev
```

### Keep consistent style by parsing code

```sh
npm run format
```

### Compile and Minify for Production

```sh
npm run build
```

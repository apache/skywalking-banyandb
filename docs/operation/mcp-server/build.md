# Build and Package

This guide is for developers who want to build the MCP server from source and create packages.

## Prerequisites

- **Node.js 20+** installed
- **npm** or **yarn** package manager
- **TypeScript** knowledge (for development)

## Project Structure

```
mcp-server/
├── src/
│   ├── index.ts              # MCP server implementation
│   ├── banyandb-client.ts    # BanyanDB HTTP client
│   ├── query-generator.ts    # Natural language to BydbQL translator
│   ├── llm-prompt.ts         # LLM prompt generation
│   └── logger.ts             # Logging utilities
├── dist/                     # Compiled JavaScript (generated)
├── Dockerfile                # Multi-stage Docker build file
├── .dockerignore             # Docker build exclusions
├── package.json
├── tsconfig.json
├── Makefile                  # Build automation
└── README.md
```

## Building from Source

### 1. Install Dependencies

```bash
cd mcp-server
npm install
```

### 2. Build

```bash
npm run build
```

This compiles TypeScript to JavaScript in the `dist/` directory.

### 3. Verify Build

```bash
node dist/index.js --help
```

## Development Mode

For development, you can use `tsx` to run TypeScript directly without compilation:

```bash
npm run dev
```

This runs `src/index.ts` directly using `tsx`, which is faster for iterative development.

## Development Workflow

### 1. Make Changes

Edit files in the `src/` directory.

### 2. Format and Lint

Before testing, format your code and check for linting issues:

```bash
npm run format
npm run lint
```

### 3. Test Changes

Use development mode for quick testing:

```bash
npm run dev
```

Or build and test:

```bash
npm run build
node dist/index.js
```

### 3. Test with Inspector

Use MCP Inspector to test your changes:

```bash
# Build first
npm run build

# Run Inspector
npx @modelcontextprotocol/inspector --config inspector-config.json
```

## Debugging

### VS Code Debug Configuration

Create `.vscode/launch.json` in the `mcp-server` directory:

```json
{
  "version": "0.1.0",
  "configurations": [
    {
      "name": "Debug with MCP Inspector",
      "type": "node",
      "request": "launch",
      "runtimeExecutable": "npx",
      "runtimeArgs": [
        "@modelcontextprotocol/inspector",
        "--config",
        "${workspaceFolder}/inspector-config.json"
      ],
      "env": {
        "BANYANDB_ADDRESS": "localhost:17900",
        "LLM_API_KEY": "${env:LLM_API_KEY}",
        "LLM_BASE_URL": "${env:LLM_BASE_URL}"
      },
      "console": "integratedTerminal",
      "internalConsoleOptions": "neverOpen",
      "skipFiles": ["<node_internals>/**"],
      "sourceMaps": true,
      "outputCapture": "std"
    }
  ]
}
```

### Debugging Steps

1. Set breakpoints in TypeScript files
2. Select "Debug with MCP Inspector" from the debug dropdown
3. Press F5 to start debugging
4. Inspector UI opens at http://localhost:6274
5. Test queries through the Inspector UI - breakpoints will trigger

## Packaging

### Create Distribution Package

The build process creates a `dist/` directory with compiled JavaScript files. To create a distributable package:

```bash
# Build
npm run build

# Create package (includes dist/ and package.json)
tar -czf banyandb-mcp-server.tar.gz dist/ package.json README.md
```

### Docker Image

The project includes a production-ready Dockerfile that uses a multi-stage build for optimal image size and security.

#### Build Docker Image

To build a Docker image:

```bash
cd mcp-server

# Build the image
docker build -t apache/skywalking-banyandb-mcp-server:latest .
```

Or using the Makefile:

```bash
# From the project root
make -C mcp-server docker

# Or from the mcp-server directory
cd mcp-server
make docker
```

#### Dockerfile Features

The Dockerfile includes:
- **Multi-stage build**: Separate build and production stages for smaller final image
- **Security**: Runs as non-root user (`appuser`) for better security
- **Optimization**: Only production dependencies in final image
- **Alpine-based**: Uses `node:20-alpine` for minimal image size

#### Build Arguments

The Dockerfile supports standard Docker build arguments. You can customize the build:

```bash
docker build \
  --build-arg NODE_ENV=production \
  -t apache/skywalking-banyandb-mcp-server:latest .
```

#### Testing the Docker Image

After building, test the image:

```bash
# Run the container
docker run --rm \
  -e BANYANDB_ADDRESS=localhost:17900 \
  -e LLM_API_KEY=your-api-key \
  -e LLM_BASE_URL=your-api-key \
  apache/skywalking-banyandb-mcp-server:latest
```

#### Publishing Docker Image

To publish the image to a registry:

```bash
# Tag the image
docker tag apache/skywalking-banyandb-mcp-server:latest \
  ghcr.io/apache/skywalking-banyandb-mcp-server:v1.0.0

# Push to registry
docker push ghcr.io/apache/skywalking-banyandb-mcp-server:v1.0.0
```

### Integration with Main Makefile

The MCP server is already integrated into the main project Makefile. The `mcp-server/Makefile` includes:

- **Build targets**: `build`, `all`, `docker`
- **Clean targets**: `clean-build`
- **Install targets**: `install`
- **License targets**: `license-check`, `license-fix`, `license-dep`
- **Version checking**: `check-version`

#### Common Makefile Commands

```bash
# Build the project (includes format check, lint, and TypeScript compilation)
make -C mcp-server build

# Build Docker image
make -C mcp-server docker

# Install dependencies
make -C mcp-server install

# Clean build artifacts
make -C mcp-server clean-build

# Check license headers
make -C mcp-server license-check

# Fix license headers
make -C mcp-server license-fix
```

The Makefile automatically handles:
- Code formatting checks (`format:check`)
- Code linting (`lint`)
- Dependency installation
- TypeScript compilation
- Docker image building (with proper tagging)
- Version management

## Testing

### Unit Tests

Currently, the project doesn't have unit tests. To add them:

1. Install a testing framework (e.g., Jest, Mocha):
   ```bash
   npm install --save-dev jest @types/jest
   ```

2. Add test script to `package.json`:
   ```json
   {
     "scripts": {
       "test": "jest"
     }
   }
   ```

3. Write tests in `src/**/*.test.ts`

### Integration Tests

Test with real BanyanDB instance:

1. Start BanyanDB:
   ```bash
   docker-compose up -d
   ```

2. Run MCP Inspector:
   ```bash
   npm run build
   npx @modelcontextprotocol/inspector --config inspector-config.json
   ```

3. Test various queries through the Inspector UI

## Code Quality

### Linting

The project includes ESLint for code quality checks. Run linting:

```bash
npm run lint
```

This will check all TypeScript files in the `src/` directory for code quality issues.

### Formatting

The project uses Prettier for code formatting. Format your code:

```bash
npm run format
```

Check formatting without making changes (useful for CI):

```bash
npm run format:check
```

Both linting and formatting are integrated into the build process via the Makefile.

## Release Process

1. **Update version** in `package.json`
2. **Format code**: `npm run format`
3. **Lint code**: `npm run lint`
4. **Build** the project: `npm run build` (or `make build` which includes lint/format checks)
5. **Test** with Inspector
6. **Create package** or Docker image
7. **Tag** the release in git
8. **Publish** Docker image (if applicable)

## Troubleshooting

**Build fails:**
- Check Node.js version: `node --version` (should be 20+)
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- Check TypeScript version compatibility

**Type errors:**
- Run `npm run build` to see all TypeScript errors
- Check `tsconfig.json` configuration

**Runtime errors:**
- Verify all dependencies are installed: `npm install`
- Check environment variables are set correctly
- Verify BanyanDB is accessible

## Resources

- [TypeScript Documentation](https://www.typescriptlang.org/docs/)
- [MCP SDK Documentation](https://github.com/modelcontextprotocol/typescript-sdk)
- [Node.js Documentation](https://nodejs.org/docs/)


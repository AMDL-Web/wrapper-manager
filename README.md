# wrapper-manager
A tool for managing multiple Wrapper instances

Support linux x86_64 and arm64 arch

## Features
- Multi-instance management
- Add accounts at runtime (support 2FA)
- Start multiple Wrapper instances for the same account by logging in again after each instance is ready
- Multi-connection decryption
- gRPC API
- Get lyrics without an account
- Automatic region detection

## Usage
```shell
Usage of ./wrapper-manager:
  -debug
        enable debug output
  -host string
        host of gRPC server (default "localhost")
  -mirror
        use mirror to download wrapper and file (for Chinese users)
  -port int
        port of gRPC server (default 8080)
  -proxy string
        proxy for wrapper and manager
```

## Deploy
For Chinese users: Please uncomment the sixth line of `Dockerfile` and configure the mirror and proxy in `docker-compose.yml`
```shell
git clone https://github.com/AMDL-Web/wrapper-manager
cd wrapper-manager
nano docker-compose.yml
docker compose up
```

## Login
You can use [WorldObservationLog/AppleMusicDecrypt](https://github.com/WorldObservationLog/AppleMusicDecrypt) `tools/login.py` to log in, or use tools such as Postman to import `proto/manager.proto` to log in. The process is as follows:
![flowchart.png](/flowchart.png)

## AMDL-Web Fork Enhancements

This repository is forked to implement several performance optimizations, thread-safety mechanisms, and functional enhancements:

- **Thread-safe Decryption Connection Pool**: Introduces a robust connection pooling mechanism to manage decryption worker connections dynamically and thread-safely under heavy parallel requests.
- **Disabled Auto-Download**: Disabled the automated downloading of wrapper binaries at startup. All dependencies are pre-configured locally to ensure offline usability, startup predictability, and deployment isolation.
- **Resilient Readiness Logic**:
  - If `instances.json` exists but contains zero instances, the server successfully initializes with `Ready = true` instead of entering a blocked state.
  - Core readiness checks and instance status transitions are fully thread-safe.
  - Hardened state transitions to prevent panics during the 2FA login verification flow.
- **Status API Extension**: The gRPC status check endpoint now returns the list of active decrypted accounts, facilitating easier integration and monitoring.
- **Upstream Adjustments & Bug Fixes**:
  - Adaptations for recent Apple token structure updates.
  - Enhanced error handling when `checkAvailableOnRegion` fails due to network issues (returns clean errors instead of crashing).
  - Fixed standard panics caused by nil interface conversions.
  - Restored missing `SelectInstance` validations for music videos.

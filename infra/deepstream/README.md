# DeepStream runtime environment

This directory is reserved for DeepStream-specific runtime packaging.

## Why there is no Dockerfile or Compose service yet

The exact NVIDIA GPU, driver, Windows/WSL2 support, and DeepStream release
must be confirmed on the development laptop before pinning a container
image. A speculative image tag or CUDA/TensorRT combination can make the
environment unusable.

Phase 0 should:

1. Record the laptop GPU, VRAM, OS, and NVIDIA driver.
2. Confirm the selected DeepStream release supports that GPU/runtime.
3. Run the matching official NVIDIA DeepStream container with GPU access.
4. Run an NVIDIA sample that decodes video and performs inference.
5. Record the exact image tag and runtime versions.
6. Only then add a pinned Dockerfile or Compose definition here.

Do not install DeepStream with an ordinary pip command. Use the installation
route documented by NVIDIA for the selected release.

## Runtime diagnostics

Run the diagnostic from inside the selected DeepStream environment:

    python -m src.deepstream.runtime_check

It checks GStreamer Python bindings and the core plugins used by the first
pipeline: nvstreammux, nvinfer, and nvv4l2decoder.

The diagnostic is not a substitute for running an NVIDIA sample application.

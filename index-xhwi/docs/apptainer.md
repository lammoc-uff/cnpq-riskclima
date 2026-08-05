# Apptainer operations

The Apptainer image derives from the CPU Docker image. This keeps Python, uv, the frozen lockfile, and system libraries in one OCI build definition rather than maintaining a separate Apptainer environment.

## Requirements

- Docker with BuildKit support
- Apptainer 1.2 or later
- Enough temporary storage for the OCI layers and the resulting SIF image

No source data, `.env` file, credentials, or generated outputs are copied into the image. Bind the required data directories when running a command.

## Build from the local Docker image

Run this from `index-xhwi`:

```bash
make apptainer-build
```

This builds the Docker `cpu` target as `riskclima-xhwi:local-cpu`, then creates `riskclima-xhwi-cpu.sif` from `docker-daemon://riskclima-xhwi:local-cpu`.

Override names or pass Apptainer build flags when needed:

```bash
make apptainer-build \
  IMAGE=registry.example.org/riskclima-xhwi \
  TAG=2026-08-04 \
  APPTAINER_IMAGE=/srv/images/riskclima-xhwi-cpu.sif \
  ARGS=--fakeroot
```

## Build from a registry

CI or a release process can publish the Docker CPU target to an OCI registry. Build the SIF directly from that immutable image without rebuilding the Python environment:

```bash
apptainer build riskclima-xhwi-cpu.sif \
  docker://registry.example.org/riskclima-xhwi@sha256:IMAGE_DIGEST
```

Use a digest for reproducible deployment. For a private registry, authenticate with `apptainer registry login` or the site-approved credential mechanism. Do not put registry credentials in `.env` or in the image.

## Run workflows

Apptainer commands execute the installed project entry points. Bind the module working directory so inputs and outputs remain on the host:

```bash
apptainer exec --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-xhwi-cpu.sif \
  riskclima-xhwi-era5-run-all --device cpu
```

Pass selected configuration variables explicitly when an `.env` file should not be exposed:

```bash
APPTAINERENV_ERA5_DEVICE=cpu \
APPTAINERENV_CDSAPI_KEY="your_key_here" \
apptainer exec --cleanenv \
  --bind "$PWD/era5:/work/era5" \
  --pwd /work \
  riskclima-xhwi-cpu.sif \
  riskclima-xhwi-era5-run-months --months-to-run 1 2 3
```

To use a host `.env`, bind that single file read-only and set the working directory where the application expects it:

```bash
apptainer exec --cleanenv \
  --bind "$PWD/.env:/work/.env:ro,$PWD/era5:/work/era5" \
  --pwd /work \
  riskclima-xhwi-cpu.sif \
  riskclima-xhwi-era5-run-all --device cpu
```

`.env` remains the canonical configuration in either mode. Supported CLI options, such as `--months-to-run` and `--device`, override only the current invocation.

## GPU execution

The standard SIF is CPU-only. If a published OCI image was built from the Docker `gpu` target, convert that OCI image with `apptainer build` and run it with NVIDIA passthrough:

```bash
apptainer exec --nv --cleanenv \
  --bind "$PWD:/work" \
  --pwd /work \
  riskclima-xhwi-gpu.sif \
  riskclima-xhwi-era5land-run-all --device cuda
```

The host driver must support CUDA 12.8. Apptainer supplies host driver libraries through `--nv`; the OCI image supplies the CUDA 12.8 runtime and the locked cu128 PyTorch wheel.

## Cluster considerations

Place SIF files in read-only shared storage when possible. Bind writable scratch and output paths per job. The pipeline may access remote ERA5 Zarr endpoints, so batch jobs need network access unless all inputs are local. The CI workflow does not run these operational commands or access external datasets.

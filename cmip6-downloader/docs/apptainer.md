# Apptainer

The Apptainer image is derived from the CPU-only Docker image; no separate definition file is maintained.

Build Docker and then convert the local image:

```bash
make apptainer-build
```

Override the output image with `make apptainer-build SIF=path/to/image.sif`. `SIF` defaults to `cmip6-downloader.sif`; `IMAGE` continues to select the Docker image tag.

Run with the configuration and data directories bound into `/app`:

```bash
apptainer run \
  --bind "$PWD/.env:/app/.env:ro" \
  --bind "$PWD/catalog:/app/catalog:ro" \
  --bind "$PWD/filtered_catalog:/app/filtered_catalog" \
  --bind "$PWD/downloads:/app/downloads" \
  --writable-tmpfs \
  cmip6-downloader.sif
```

Remote AWS and Google access requires outbound network access. On clusters, bind a high-capacity scratch directory to `/tmp` when the default temporary filesystem is small, for example `--bind "$SCRATCH:/tmp"`. Add script options after the image path. To run catalog comparison instead, use `apptainer exec ... /app/.venv/bin/python scripts/compare_catalogs.py`.

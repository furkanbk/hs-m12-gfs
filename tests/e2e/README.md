# End-to-end integration tests

Owner: **Mikita**. These tests run against the **whole 5-service stack** and
verify both the happy-path file operations and the fault-tolerance guarantees in
[`docs/ARCHITECTURE.md` §5](../../docs/ARCHITECTURE.md#5-fault-tolerance).

- `test_file_operations.py` — create / read / size / delete, chunk-boundary,
  empty file, UTF-8, duplicate-name, and 404 cases, all through the client API.
- `test_fault_tolerance.py` — stops/starts containers to prove reads survive up
  to 2 of 3 storage servers down, writes fail strictly while a node is down, and
  naming-server metadata survives a restart.

## Run it (managed stack — default)

The suite brings the stack up (`docker compose up -d --build --wait`) and tears
it down afterwards. From the repo root:

```bash
pip install -r tests/e2e/requirements.txt
# If host port 8080 is busy, pick another:
NARANJA_CLIENT_PORT=8088 python -m pytest tests/e2e -v
```

## Run it against an already-running stack

```bash
docker compose up -d --build
NARANJA_MANAGE_STACK=0 NARANJA_BASE_URL=http://localhost:8080 \
  python -m pytest tests/e2e/test_file_operations.py -v
```

(The fault-tolerance tests need to control containers, so they self-skip when
`NARANJA_MANAGE_STACK=0`.)

## Environment knobs

| Var | Default | Meaning |
| --- | --- | --- |
| `NARANJA_CLIENT_PORT` | `8080` | Host port the client UI is published on. |
| `NARANJA_BASE_URL` | `http://localhost:<port>` | Full base URL of the client. |
| `NARANJA_COMPOSE_PROJECT` | `naranja-e2e` | Compose project name (isolation). |
| `NARANJA_MANAGE_STACK` | `1` | `1` = bring stack up/down here; `0` = reuse. |

import os
import subprocess
import sys

from main import run_pipeline


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def run_etl() -> None:
    with_photos = _env_bool("WITH_PHOTOS", False)
    photo_limit = int(os.getenv("PHOTO_LIMIT", "200"))
    run_pipeline(with_photos=with_photos, photo_limit=photo_limit)


def run_streamlit() -> None:
    port = os.getenv("PORT", os.getenv("STREAMLIT_PORT", "8080"))
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        "etl/ui_app.py",
        "--server.address",
        "0.0.0.0",
        "--server.port",
        str(port),
    ]
    raise SystemExit(subprocess.run(command, check=False).returncode)


def main() -> None:
    mode = os.getenv("APP_MODE", "etl").strip().lower()

    if mode == "etl":
        run_etl()
        return

    if mode == "streamlit":
        run_streamlit()
        return

    raise SystemExit(f"APP_MODE no soportado: {mode}")


if __name__ == "__main__":
    main()
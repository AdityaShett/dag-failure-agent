import os
import json

from google.cloud import secretmanager

# Cache the parsed config and the Secret Manager client across invocations
# (Cloud Run reuses warm instances) so we're not re-reading disk or
# re-authenticating on every single request.
_CONFIG_CACHE = None
_SECRET_CLIENT = None
_TOKEN_CACHE = {}  # repo_key -> token, cleared only on cold start


def _get_secret_client():
    global _SECRET_CLIENT
    if _SECRET_CLIENT is None:
        _SECRET_CLIENT = secretmanager.SecretManagerServiceClient()
    return _SECRET_CLIENT


def _load_repo_config() -> dict:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    config_path = os.environ.get("REPO_CONFIG_PATH", "config/repos.json")
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"repo config not found at '{config_path}'. Set REPO_CONFIG_PATH "
            f"or add config/repos.json to the deploy bundle."
        )

    with open(config_path, "r") as f:
        _CONFIG_CACHE = json.load(f)

    return _CONFIG_CACHE


def _access_secret(secret_name: str) -> str:
    project_id = os.environ.get("GCP_PROJECT")
    if not project_id:
        raise ValueError("GCP_PROJECT env var is not set; cannot resolve secret.")

    version_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = _get_secret_client().access_secret_version(name=version_name)
    return response.payload.data.decode("utf-8")


def get_github_token(github_repo: str) -> str:
    """
    Resolve the GitHub token for a given 'owner/repo' string.

    Lookup order:
      1. Exact match on github_repo in config/repos.json
      2. The "default" entry in config/repos.json
      3. Legacy fallback: the GITHUB_TOKEN env var, if set.

    (3) exists so a half-migrated deploy doesn't hard-fail -- but once every
    repo you actually use is listed in config/repos.json, GITHUB_TOKEN can
    (and should) be removed from your Cloud Run env vars entirely, so a
    misconfigured repo fails loudly instead of silently using the wrong
    token.
    """
    if github_repo in _TOKEN_CACHE:
        return _TOKEN_CACHE[github_repo]

    config = _load_repo_config()
    entry = config.get(github_repo) or config.get("default")

    if entry is None:
        legacy_token = os.environ.get("GITHUB_TOKEN")
        if legacy_token:
            print(
                f"WARNING: no repo_config entry for '{github_repo}' and no "
                f"'default' set -- using legacy GITHUB_TOKEN env var. Add "
                f"'{github_repo}' to config/repos.json."
            )
            return legacy_token
        raise ValueError(
            f"No repo config entry for '{github_repo}', no 'default' entry, "
            f"and no GITHUB_TOKEN fallback set. Add it to config/repos.json."
        )

    secret_name = entry.get("token_secret")
    if not secret_name:
        raise ValueError(
            f"repo config entry for '{github_repo}' is missing 'token_secret'."
        )

    token = _access_secret(secret_name)
    _TOKEN_CACHE[github_repo] = token
    return token
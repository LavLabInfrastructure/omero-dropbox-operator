from __future__ import annotations

import copy
import logging
import os
from typing import Any, Dict, Tuple, cast

import yaml
from flask import Flask, jsonify, request
from kubernetes import client, config
from kubernetes.client import ApiException, V1ConfigMap, V1Job
from kubernetes.config import ConfigException

DEFAULT_NAMESPACE = "omero-dropbox-system"
NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
DEFAULT_CONFIG_MAP_NAME = "default-import-job-config"
JOB_GENERATE_NAME_PREFIX = "import-job-"
WATCH_VOLUME_NAME = "work-volume"
WATCH_CONTAINER_PATH = "/data"

LOG = logging.getLogger(__name__)


def load_kubernetes_configuration() -> None:
    """Load Kubernetes configuration for either in-cluster or local development."""
    try:
        config.load_incluster_config()
        LOG.debug("Loaded in-cluster Kubernetes configuration.")
    except ConfigException:
        config.load_kube_config()
        LOG.debug("Loaded local kubeconfig (development mode).")


def discover_namespace(default: str = DEFAULT_NAMESPACE) -> str:
    """Discover the namespace the webhook should operate in."""
    try:
        with open(NAMESPACE_PATH, "r", encoding="utf-8") as stream:
            value = stream.read().strip()
            return value or default
    except FileNotFoundError:
        return os.environ.get("POD_NAMESPACE", default)
    except OSError as exc:
        LOG.warning("Falling back to default namespace after reading %s failed: %s", NAMESPACE_PATH, exc)
        return default


load_kubernetes_configuration()
app = Flask(__name__)
namespace = discover_namespace()
omero_dropbox_name = os.environ.get("WATCH_NAME", "").strip()


def get_config_map_data(namespace: str, name: str) -> Dict[str, Any]:
    """Fetch and parse the job configuration from a ConfigMap."""
    if not name:
        raise RuntimeError("ConfigMap name must be provided.")

    core_v1 = client.CoreV1Api()
    try:
        config_map = cast(V1ConfigMap, core_v1.read_namespaced_config_map(name, namespace))
    except ApiException as exc:
        raise RuntimeError(f"Unable to read ConfigMap {name!r} in namespace {namespace!r}: {exc}") from exc

    config_data = (config_map.data or {}).get("config.yaml")
    if not config_data:
        raise RuntimeError(f"ConfigMap {name!r} does not contain a 'config.yaml' key.")

    try:
        parsed = yaml.safe_load(config_data) or {}
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Failed to parse YAML from ConfigMap {name!r}: {exc}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError(f"ConfigMap {name!r} content must deserialize to a dictionary.")

    return parsed


def get_omero_dropbox_crd(namespace: str, name: str) -> Dict[str, Any]:
    """Fetch the OmeroDropbox custom resource for runtime configuration."""
    if not name:
        raise RuntimeError("WATCH_NAME environment variable is not set.")

    custom_api = client.CustomObjectsApi()
    try:
        return custom_api.get_namespaced_custom_object(
            group="omero.lavlab.edu",
            version="v1",
            namespace=namespace,
            plural="omerodropboxes",
            name=name,
        )
    except ApiException as exc:
        raise RuntimeError(f"Unable to read OmeroDropbox CRD {name!r}: {exc}") from exc


def merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries without mutating the originals."""
    result = copy.deepcopy(base) if isinstance(base, dict) else {}
    for key, value in (override or {}).items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_dicts(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def build_env(job_config: Dict[str, Any], work_path: str) -> list[Dict[str, Any]]:
    env: list[Dict[str, Any]] = [copy.deepcopy(entry) for entry in job_config.get("env", [])]
    env.append({"name": "FILE", "value": work_path})

    omero_user_secret = job_config.get("omeroUserSecret", {})
    for key, secret in omero_user_secret.items():
        if not isinstance(secret, dict):
            continue
        secret_name = secret.get("name")
        secret_key = secret.get("key")
        if not secret_name or not secret_key:
            LOG.warning("Skipping malformed secret mapping for key %s", key)
            continue
        env.append(
            {
                "name": f"OMERO_{key.upper()}",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": secret_name,
                        "key": secret_key,
                    }
                },
            }
        )

    return env


def build_volumes(job_config: Dict[str, Any], pvc_name: str) -> Tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    volumes = [
        {
            "name": WATCH_VOLUME_NAME,
            "persistentVolumeClaim": {"claimName": pvc_name},
        }
    ]
    volume_mounts = [
        {"name": WATCH_VOLUME_NAME, "mountPath": WATCH_CONTAINER_PATH},
    ]

    for additional in job_config.get("additionalVolumes", []) or []:
        if isinstance(additional, dict):
            volumes.append(copy.deepcopy(additional))

    for additional in job_config.get("additionalMounts", []) or []:
        if isinstance(additional, dict):
            volume_mounts.append(copy.deepcopy(additional))

    return volumes, volume_mounts


def create_job(namespace: str, job_config: Dict[str, Any], pvc_name: str, work_path: str) -> str:
    image = job_config.get("image")
    command = job_config.get("command")
    if not image:
        raise RuntimeError("Job configuration is missing required 'image' field.")
    if not command:
        raise RuntimeError("Job configuration is missing required 'command' field.")

    env = build_env(job_config, work_path)
    volumes, volume_mounts = build_volumes(job_config, pvc_name)
    resources = job_config.get("resources", {})

    metadata: Dict[str, Any] = copy.deepcopy(job_config.get("metadata", {}))
    labels = copy.deepcopy(job_config.get("labels", {}))
    annotations = copy.deepcopy(job_config.get("annotations", {}))

    if labels:
        metadata.setdefault("labels", {}).update(labels)
    if annotations:
        metadata.setdefault("annotations", {}).update(annotations)
    if "name" not in metadata:
        metadata.setdefault("generateName", JOB_GENERATE_NAME_PREFIX)

    ttl_seconds = job_config.get("ttlSecondsAfterFinished", 3600)
    backoff_limit = job_config.get("backoffLimit", 3)
    service_account = job_config.get("serviceAccountName")

    template_spec: Dict[str, Any] = {
        "containers": [
            {
                "name": "worker",
                "image": image,
                "command": command,
                "env": env,
                "volumeMounts": volume_mounts,
                "resources": resources,
            }
        ],
        "volumes": volumes,
        "restartPolicy": "Never",
    }
    if service_account:
        template_spec["serviceAccountName"] = service_account

    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "ttlSecondsAfterFinished": ttl_seconds,
            "backoffLimit": backoff_limit,
            "template": {
                "spec": template_spec,
            },
        },
    }

    batch_v1 = client.BatchV1Api()
    try:
        job = cast(V1Job, batch_v1.create_namespaced_job(namespace=namespace, body=job_spec))
    except ApiException as exc:
        raise RuntimeError(f"Failed to create Job: {exc}") from exc

    job_name = getattr(getattr(job, "metadata", None), "name", None)
    if not job_name:
        raise RuntimeError("Kubernetes Job response did not include a name.")
    return job_name


def resolve_pvc_info(dropbox: Dict[str, Any]) -> Tuple[str, str]:
    try:
        pvc = dropbox["spec"]["watch"]["watched"]["pvc"]
        pvc_name = pvc["name"]
        pvc_path = pvc.get("path", "/")
    except (KeyError, TypeError):
        raise RuntimeError("OmeroDropbox spec is missing watched PVC information.")

    return pvc_name, pvc_path


def perform_health_checks() -> Tuple[Dict[str, bool], Dict[str, str]]:
    checks: Dict[str, bool] = {
        "kubernetes": True,
        "defaultConfigMap": True,
    }
    errors: Dict[str, str] = {}

    try:
        version = client.VersionApi().get_code()
        LOG.debug("Connected to Kubernetes API version %s", getattr(version, "git_version", "unknown"))
    except ApiException as exc:
        checks["kubernetes"] = False
        errors["kubernetes"] = f"{exc.status}: {exc.reason}"
    except Exception as exc:  # pragma: no cover - defensive
        checks["kubernetes"] = False
        errors["kubernetes"] = str(exc)

    try:
        get_config_map_data(namespace, DEFAULT_CONFIG_MAP_NAME)
    except Exception as exc:  # broad but reported via errors
        checks["defaultConfigMap"] = False
        errors["defaultConfigMap"] = str(exc)

    if omero_dropbox_name:
        try:
            get_omero_dropbox_crd(namespace, omero_dropbox_name)
            checks["dropboxCRD"] = True
        except Exception as exc:
            checks["dropboxCRD"] = False
            errors["dropboxCRD"] = str(exc)
    else:
        checks["dropboxCRD"] = False
        errors["dropboxCRD"] = "WATCH_NAME environment variable not set"

    return checks, errors


@app.route("/import", methods=["POST"])
def import_handler():
    if not request.is_json:
        return jsonify({"error": "Expected JSON body."}), 400

    payload = request.get_json(silent=True) or {}
    full_path = payload.get("fullPath")
    if not full_path:
        return jsonify({"error": "'fullPath' is required."}), 400

    try:
        default_job_config = get_config_map_data(namespace, DEFAULT_CONFIG_MAP_NAME)
        dropbox_crd = get_omero_dropbox_crd(namespace, omero_dropbox_name)
        specific_config_name = dropbox_crd.get("spec", {}).get("watch", {}).get("configMapName")
        specific_job_config = (
            get_config_map_data(namespace, specific_config_name) if specific_config_name else {}
        )
        job_config = merge_dicts(default_job_config, specific_job_config)
        pvc_name, pvc_path = resolve_pvc_info(dropbox_crd)
    except RuntimeError as exc:
        LOG.error("Import request failed preflight: %s", exc)
        return jsonify({"error": str(exc)}), 500

    if full_path.startswith("/watch"):
        relative_path = full_path[len("/watch"):]
    else:
        relative_path = full_path
    relative_path = relative_path.lstrip("/")

    base_path = WATCH_CONTAINER_PATH.rstrip("/") or "/"
    if not base_path.startswith("/"):
        base_path = f"/{base_path}"

    work_path_in_pod = base_path if not relative_path else f"{base_path}/{relative_path}"

    try:
        job_name = create_job(namespace, job_config, pvc_name, work_path_in_pod)
    except RuntimeError as exc:
        LOG.error("Failed to create import job: %s", exc)
        return jsonify({"error": str(exc)}), 500

    return jsonify({"message": "Job created successfully", "jobName": job_name}), 202


@app.route("/ready", methods=["GET"])
def ready_handler():
    checks, errors = perform_health_checks()
    required = checks.get("kubernetes") and checks.get("defaultConfigMap")
    status_code = 200 if required else 503
    body = {"status": "ready" if required else "not_ready", "checks": checks}
    if not required:
        body["errors"] = errors
    return jsonify(body), status_code


@app.route("/healthz", methods=["GET"])
def health_handler():
    checks, errors = perform_health_checks()
    healthy = all(checks.values())
    status_code = 200 if healthy else 503
    body = {"status": "ok" if healthy else "unhealthy", "checks": checks}
    if errors:
        body["errors"] = errors
    return jsonify(body), status_code


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
from contextlib import AsyncExitStack
from typing import Any, Callable, Iterable, Mapping

import kopf
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiException
from kubernetes_asyncio.config import ConfigException

DEFAULT_NAMESPACE = "omero-dropbox-system"
DEFAULT_OPERATOR_IMAGE = "omero-dropbox-operator:latest"
NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
JOB_MANAGED_LABEL = "omero.lavlab.edu/managed-by"
JOB_WATCH_LABEL = "omero.lavlab.edu/watch-name"
JOB_MANAGED_LABEL_VALUE = "omero-dropbox-operator"
POD_DELETION_INTERVAL = 2
POD_DELETION_TIMEOUT = 120
JOB_GENERATED_PREFIX = "import-job-"
WEBHOOK_PORT = 8080
WEBHOOK_LIVENESS_PATH = "/healthz"
WEBHOOK_READINESS_PATH = "/ready"

LOG = logging.getLogger(__name__)


async def call_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Invoke a callable and await the result when needed."""
    result = func(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def discover_namespace(default: str = DEFAULT_NAMESPACE) -> str:
    """Resolve the namespace the operator should use."""
    try:
        with open(NAMESPACE_PATH, "r", encoding="utf-8") as stream:
            namespace = stream.read().strip()
            return namespace or default
    except FileNotFoundError:
        return os.environ.get("POD_NAMESPACE", default)
    except OSError as exc:
        LOG.warning("Falling back to default namespace after reading %s failed: %s", NAMESPACE_PATH, exc)
        return default


async def load_kube_configuration(logger: logging.Logger) -> None:
    """Load Kubernetes configuration, preferring in-cluster credentials."""
    try:
        await call_async(config.load_incluster_config)
        logger.info("Loaded in-cluster Kubernetes configuration.")
    except ConfigException:
        await call_async(config.load_kube_config)
        logger.info("Loaded local kubeconfig (development mode).")


async def get_operator_image(core_api: client.CoreV1Api, namespace: str, logger: logging.Logger) -> str:
    pod_name = os.environ.get("HOSTNAME")
    if not pod_name:
        logger.warning("HOSTNAME environment variable missing; using default operator image.")
        return DEFAULT_OPERATOR_IMAGE

    try:
        pod = await call_async(core_api.read_namespaced_pod, name=pod_name, namespace=namespace)
    except ApiException as exc:
        logger.warning("Unable to resolve operator image from pod %s: %s", pod_name, exc)
        return DEFAULT_OPERATOR_IMAGE

    containers = pod.spec.containers or []
    if not containers:
        logger.warning("Operator pod %s has no containers; using default operator image.", pod_name)
        return DEFAULT_OPERATOR_IMAGE

    image = containers[0].image or DEFAULT_OPERATOR_IMAGE
    if image == DEFAULT_OPERATOR_IMAGE:
        logger.info("Default operator image in use: %s", image)
    return image


def _normalise_subpath(subpath: str | None) -> str:
    if not subpath:
        return ""
    trimmed = subpath.lstrip("/")
    return f"/{trimmed}" if trimmed else ""


def build_watch_container(image: str, subpath: str) -> Mapping[str, Any]:
    # Mounted path should match watcher expectations.
    return {
        "name": "watch",
        "image": image,
        "env": [
            {"name": "MODE", "value": "WATCH"},
            {"name": "WATCHED_DIR", "value": f"/watch{subpath}"},
        ],
        "volumeMounts": [{"name": "watched-volume", "mountPath": "/watch"}],
    }


def build_webhook_container(image: str, name: str) -> Mapping[str, Any]:
    return {
        "name": "webhook",
        "image": image,
        "env": [
            {"name": "MODE", "value": "WEBHOOK"},
            {"name": "WATCH_NAME", "value": name},
        ],
        "ports": [
            {
                "name": "http",
                "containerPort": WEBHOOK_PORT,
            }
        ],
        "livenessProbe": {
            "httpGet": {
                "path": WEBHOOK_LIVENESS_PATH,
                "port": "http",
            },
            "initialDelaySeconds": 10,
            "periodSeconds": 20,
        },
        "readinessProbe": {
            "httpGet": {
                "path": WEBHOOK_READINESS_PATH,
                "port": "http",
            },
            "initialDelaySeconds": 5,
            "periodSeconds": 10,
        },
    }


def build_dropbox_pod_manifest(
    name: str,
    namespace: str,
    watch_spec: Mapping[str, Any],
    operator_image: str,
) -> Mapping[str, Any]:
    watched = watch_spec.get("watched")
    if not isinstance(watched, Mapping):
        raise kopf.PermanentError("spec.watch.watched must be provided.")

    pvc_spec = watched.get("pvc")
    if not isinstance(pvc_spec, Mapping):
        raise kopf.PermanentError("spec.watch.watched.pvc must be provided.")

    pvc_name = pvc_spec.get("name")
    if not pvc_name:
        raise kopf.PermanentError("spec.watch.watched.pvc.name is required.")

    pvc_path = pvc_spec.get("path")
    subpath = _normalise_subpath(pvc_path)

    watch_container = build_watch_container(operator_image, subpath)
    webhook_container = build_webhook_container(operator_image, name)

    volumes = [
        {
            "name": "watched-volume",
            "persistentVolumeClaim": {"claimName": pvc_name},
        }
    ]

    labels = {
        JOB_MANAGED_LABEL: JOB_MANAGED_LABEL_VALUE,
        JOB_WATCH_LABEL: name,
    }

    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-watch",
            "namespace": namespace,
            "labels": labels,
        },
        "spec": {
            "serviceAccountName": "omero-dropbox-webhook",
            "containers": [watch_container, webhook_container],
            "volumes": volumes,
            "restartPolicy": "OnFailure",
        },
    }


def _env_signature(env: Iterable[Any] | None) -> list[tuple[str, tuple[str, str | None]]]:
    signature: list[tuple[str, tuple[str, str | None]]] = []
    for item in env or []:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if not name:
            continue
        if "value" in item:
            signature.append((name, ("value", item.get("value"))))
        elif "valueFrom" in item:
            signature.append((name, ("valueFrom", json.dumps(item.get("valueFrom"), sort_keys=True))))
        else:
            signature.append((name, ("none", None)))
    signature.sort(key=lambda pair: pair[0])
    return signature


def _volume_mount_signature(volume_mounts: Iterable[Any] | None) -> list[tuple[str, str | None, str | None]]:
    signature: list[tuple[str, str | None, str | None]] = []
    for item in volume_mounts or []:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if not name:
            continue
        signature.append((name, item.get("mountPath"), item.get("subPath")))
    signature.sort(key=lambda pair: pair[0])
    return signature


def _volume_signature(volumes: Iterable[Any] | None) -> list[tuple[str, str | None]]:
    signature: list[tuple[str, str | None]] = []
    for item in volumes or []:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if not name:
            continue
        pvc = item.get("persistentVolumeClaim") or {}
        claim = None
        if isinstance(pvc, Mapping):
            claim = pvc.get("claimName")
        signature.append((name, claim))
    signature.sort(key=lambda pair: pair[0])
    return signature


def pod_needs_update(existing_pod: Any, desired_manifest: Mapping[str, Any]) -> bool:
    desired_spec = desired_manifest["spec"]
    desired_containers = {c["name"]: c for c in desired_spec.get("containers", [])}

    existing_containers = {}
    for container in existing_pod.spec.containers or []:
        existing_containers[container.name] = container

    if set(desired_containers) != set(existing_containers):
        return True

    for name, desired_container in desired_containers.items():
        existing_container = existing_containers[name]
        if existing_container.image != desired_container.get("image"):
            return True
        if _env_signature(existing_container.env) != _env_signature(desired_container.get("env")):
            return True
        if _volume_mount_signature(existing_container.volume_mounts) != _volume_mount_signature(
            desired_container.get("volumeMounts")
        ):
            return True

    if _volume_signature(existing_pod.spec.volumes) != _volume_signature(desired_spec.get("volumes")):
        return True

    return False


async def wait_for_pod_absence(
    core_api: client.CoreV1Api,
    pod_name: str,
    namespace: str,
    logger: logging.Logger,
) -> None:
    deadline = asyncio.get_event_loop().time() + POD_DELETION_TIMEOUT
    while True:
        try:
            await call_async(core_api.read_namespaced_pod, name=pod_name, namespace=namespace)
        except ApiException as exc:
            if exc.status == 404:
                return
            raise
        if asyncio.get_event_loop().time() >= deadline:
            raise kopf.TemporaryError(
                f"Timed out waiting for pod {pod_name} to be deleted.",
                delay=5,
            )
        await asyncio.sleep(POD_DELETION_INTERVAL)
        logger.debug("Waiting for pod %s to terminate...", pod_name)


async def create_pod(core_api: client.CoreV1Api, manifest: Mapping[str, Any], logger: logging.Logger) -> None:
    pod_name = manifest["metadata"]["name"]
    namespace = manifest["metadata"]["namespace"]
    try:
        await call_async(core_api.create_namespaced_pod, namespace=namespace, body=manifest)
        logger.info("Created pod %s in namespace %s.", pod_name, namespace)
    except ApiException as exc:
        if exc.status == 409:
            raise kopf.TemporaryError(
                f"Pod {pod_name} already exists. Retrying shortly.",
                delay=5,
            ) from exc
        raise


async def delete_pod_if_exists(
    core_api: client.CoreV1Api,
    pod_name: str,
    namespace: str,
    logger: logging.Logger,
) -> None:
    try:
        await call_async(core_api.delete_namespaced_pod, name=pod_name, namespace=namespace)
        logger.info("Deleted pod %s in namespace %s.", pod_name, namespace)
    except ApiException as exc:
        if exc.status in (404, 410):
            logger.debug("Pod %s already absent in namespace %s.", pod_name, namespace)
            return
        raise
    await wait_for_pod_absence(core_api, pod_name, namespace, logger)


async def ensure_dropbox_pod(
    *,
    name: str,
    resource_namespace: str,
    spec: Mapping[str, Any] | None,
    memo: kopf.Memo,
    logger: logging.Logger,
    force_restart: bool = False,
) -> None:
    if spec is None:
        raise kopf.PermanentError("spec must be present on the OmeroDropbox resource.")

    watch_spec = spec.get("watch")
    if not isinstance(watch_spec, Mapping):
        raise kopf.PermanentError("spec.watch must be provided.")

    operator_namespace: str = memo["namespace"]
    core_api: client.CoreV1Api = memo["core_api"]
    operator_image: str = memo["operator_image"]

    manifest = build_dropbox_pod_manifest(
        name=name,
        namespace=operator_namespace,
        watch_spec=watch_spec,
        operator_image=operator_image,
    )

    pod_name = manifest["metadata"]["name"]

    async with memo["lock"]:
        if force_restart:
            await delete_pod_if_exists(core_api, pod_name, operator_namespace, logger)
            await create_pod(core_api, manifest, logger)
            return

        try:
            existing_pod = await call_async(core_api.read_namespaced_pod, name=pod_name, namespace=operator_namespace)
        except ApiException as exc:
            if exc.status == 404:
                await create_pod(core_api, manifest, logger)
                return
            raise

        if pod_needs_update(existing_pod, manifest):
            logger.info(
                "Pod %s out of date for dropbox %s/%s; replacing.",
                pod_name,
                resource_namespace,
                name,
            )
            await delete_pod_if_exists(core_api, pod_name, operator_namespace, logger)
            await create_pod(core_api, manifest, logger)
        else:
            logger.debug("Pod %s already aligned with desired configuration.", pod_name)


async def cleanup_job(job_name: str, namespace: str, memo: kopf.Memo, logger: logging.Logger) -> None:
    batch_api: client.BatchV1Api | None = memo.get("batch_api")
    if batch_api is None:
        logger.warning("Batch API client unavailable; cannot clean up job %s.", job_name)
        return

    try:
        await call_async(
            batch_api.delete_namespaced_job,
            name=job_name,
            namespace=namespace,
            propagation_policy="Background",
        )
        logger.info("Cleaned up job %s in namespace %s.", job_name, namespace)
    except ApiException as exc:
        if exc.status in (404, 410):
            logger.debug("Job %s already absent in namespace %s.", job_name, namespace)
        else:
            raise


@kopf.on.startup()  # type: ignore[misc]
async def startup(
    settings: kopf.OperatorSettings,
    memo: kopf.Memo,
    logger: logging.Logger,
) -> None:
    settings.posting.level = logging.INFO
    settings.persistence.finalizer = "omero.lavlab.edu/finalizer"

    namespace = discover_namespace()
    memo["namespace"] = namespace
    memo["lock"] = asyncio.Lock()

    stack = AsyncExitStack()
    await stack.__aenter__()
    memo["stack"] = stack

    await load_kube_configuration(logger)

    api_client = await stack.enter_async_context(client.ApiClient())
    memo["api_client"] = api_client
    memo["core_api"] = client.CoreV1Api(api_client)
    memo["batch_api"] = client.BatchV1Api(api_client)
    memo["custom_api"] = client.CustomObjectsApi(api_client)

    memo["operator_image"] = await get_operator_image(memo["core_api"], namespace, logger)
    logger.info("Operator started in namespace %s using image %s.", namespace, memo["operator_image"])

    custom_api: client.CustomObjectsApi = memo["custom_api"]
    try:
        dropboxes = await call_async(
            custom_api.list_namespaced_custom_object,
            group="omero.lavlab.edu",
            version="v1",
            namespace=namespace,
            plural="omerodropboxes",
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
        logger.info("No existing OmeroDropbox resources found during startup.")
        return

    for dropbox in dropboxes.get("items", []):
        drop_name = dropbox.get("metadata", {}).get("name")
        drop_namespace = dropbox.get("metadata", {}).get("namespace", namespace)
        drop_spec = dropbox.get("spec")
        if not drop_name:
            continue
        try:
            await ensure_dropbox_pod(
                name=drop_name,
                resource_namespace=drop_namespace,
                spec=drop_spec,
                memo=memo,
                logger=logger,
            )
        except kopf.PermanentError as exc:
            logger.error("Skipping OmeroDropbox %s due to invalid spec: %s", drop_name, exc)


@kopf.on.cleanup()  # type: ignore[misc]
async def shutdown(memo: kopf.Memo, logger: logging.Logger) -> None:
    stack: AsyncExitStack | None = memo.pop("stack", None)
    if stack is not None:
        await stack.aclose()
    logger.info("Operator shutdown complete.")


@kopf.on.update("omero.lavlab.edu", "v1", "omerodropboxes")  # type: ignore[misc]
async def on_update(
    name: str,
    namespace: str,
    spec: Mapping[str, Any],
    diff: list[tuple[str, tuple[str, ...], Any, Any]],
    memo: kopf.Memo,
    logger: logging.Logger,
) -> None:
    significant_change = any(
        op in {"add", "change", "remove"} and path and path[0] == "spec"
        for op, path, *_ in diff
    )
    await ensure_dropbox_pod(
        name=name,
        resource_namespace=namespace,
        spec=spec,
        memo=memo,
        logger=logger,
        force_restart=significant_change,
    )


@kopf.on.create("omero.lavlab.edu", "v1", "omerodropboxes")  # type: ignore[misc]
async def on_create(
    spec: Mapping[str, Any],
    name: str,
    namespace: str,
    memo: kopf.Memo,
    logger: logging.Logger,
) -> None:
    logger.info("Creating resources for OmeroDropbox %s in namespace %s.", name, namespace)
    await ensure_dropbox_pod(
        name=name,
        resource_namespace=namespace,
        spec=spec,
        memo=memo,
        logger=logger,
        force_restart=True,
    )


@kopf.on.delete("omero.lavlab.edu", "v1", "omerodropboxes")  # type: ignore[misc]
async def on_delete(
    name: str,
    namespace: str,
    memo: kopf.Memo,
    logger: logging.Logger,
) -> None:
    logger.info("Deleting resources for OmeroDropbox %s in namespace %s.", name, namespace)
    core_api: client.CoreV1Api | None = memo.get("core_api")
    operator_namespace: str = memo.get("namespace", DEFAULT_NAMESPACE)
    if core_api is None:
        logger.warning("CoreV1Api client unavailable; unable to delete pod for %s.", name)
        return

    pod_name = f"{name}-watch"
    try:
        await delete_pod_if_exists(core_api, pod_name, operator_namespace, logger)
    except ApiException as exc:
        if exc.status not in (404, 410):
            raise


@kopf.on.event("batch", "v1", "jobs")  # type: ignore[misc]
async def on_job_event(
    namespace: str | None,
    event: Mapping[str, Any],
    memo: kopf.Memo,
    logger: logging.Logger,
) -> None:
    job = event.get("object")
    if not job:
        return

    metadata = job.get("metadata", {})
    job_name = metadata.get("name")
    if not job_name:
        return

    labels = metadata.get("labels", {})
    should_handle = (
        labels.get(JOB_MANAGED_LABEL) == JOB_MANAGED_LABEL_VALUE
        or JOB_WATCH_LABEL in labels
        or job_name.startswith(JOB_GENERATED_PREFIX)
    )
    if not should_handle:
        return

    job_namespace = metadata.get("namespace") or namespace or memo.get("namespace", DEFAULT_NAMESPACE)
    status = job.get("status", {}) or {}
    conditions = status.get("conditions", []) or []
    condition_status = {cond.get("type"): cond.get("status") for cond in conditions if isinstance(cond, Mapping)}

    event_type = event.get("type")
    if event_type == "DELETED":
        logger.info("Job %s deleted in namespace %s.", job_name, job_namespace)
        return

    if condition_status.get("Complete") == "True":
        logger.info("Job %s completed successfully in namespace %s.", job_name, job_namespace)
        await cleanup_job(job_name, job_namespace, memo, logger)
    elif condition_status.get("Failed") == "True":
        logger.info("Job %s failed in namespace %s; scheduling cleanup.", job_name, job_namespace)
        await cleanup_job(job_name, job_namespace, memo, logger)

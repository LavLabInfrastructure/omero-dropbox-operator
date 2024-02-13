import os
import kopf
import yaml
import asyncio
# from kubernetes import client, config
from kubernetes_asyncio import client as client, config
from kubernetes_asyncio.client.rest import ApiException

async def get_operator_image():
    pod_name = os.environ.get('HOSTNAME')
    async with client.CoreV1Api() as api:
        try:
            pod = await api.read_namespaced_pod(name=pod_name, namespace=OPERATOR_NAMESPACE)
            operator_image = pod.spec.containers[0].image
        except ApiException as e:
            print(f"Failed to get operator image: {e}")
            operator_image = 'omero-dropbox-operator:latest'
    return operator_image

def _prepare_watch_container(image, subpath=''):
    subpath=subpath.removeprefix('/')
    if subpath != '':
        subpath = f"/{subpath}"
    env = [
        {"name": "MODE", "value": "WATCH"},
        {"name": "WATCHED_DIR", "value": f"/watch{subpath}"}
    ]
    return {
        "name": "watch",
        "image": image,
        "env": env,
        "volumeMounts": [{"name": "watched-volume","mountPath": "/watch"}]
    }

def _prepare_webhook_container(image, name):
    env = [
        {"name": "MODE", "value": "WEBHOOK"},
        {"name": "WATCH_NAME", "value": name}
    ]
    return {
        "name": "webhook",
        "image": image,
        "env": env
    }

def create_dropbox_pod_manifest(name, watch):
    volumes, watch_manifest, webhook_manifest = None
    if 'pvc' in watch['watched']:
        watched_pvc_name = watch['watched']['pvc']['name']
        watched_pvc_path = watch['watched']['pvc'].get('path', '/')

        watch_manifest = _prepare_watch_container(OPERATOR_IMAGE, watched_pvc_path)
        webhook_manifest = _prepare_webhook_container(OPERATOR_IMAGE, name)

        volumes = [{
            "name": "watched-volume",
            "persistentVolumeClaim": {"claimName": watched_pvc_name}
        }]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{name}-watch", "namespace": OPERATOR_NAMESPACE},
        "spec": {
            "serviceAccountName": 'omero-dropbox-webhook',
            "containers": [watch_manifest, webhook_manifest],
            "volumes": volumes,
            "restartPolicy": "OnFailure"
        }
    }

async def create_pod(pod_manifest, logger, name):
    pod_name = f"{name}-watch"
    namespace = OPERATOR_NAMESPACE
    async with client.CoreV1Api() as api:
        try:
            await api.read_namespaced_pod(name=pod_name, namespace=namespace)
            logger.info(f"Pod {pod_name} already exists in namespace {namespace}. Skipping creation.")
        except ApiException as e:
            if e.status == 404:  # Not found, safe to create
                try:
                    await api.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                    logger.info(f"Pod {pod_name} created in namespace {namespace}.")
                except ApiException as create_error:
                    logger.error(f"Failed to create Pod {pod_name}: {create_error}")
            else:
                logger.error(f"Failed to check existence of Pod {pod_name}: {e}")
    
# globals
OPERATOR_NAMESPACE = 'omero-dropbox-system'
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()

OPERATOR_IMAGE = get_operator_image()

@kopf.on.startup()
async def startup_fn(logger, **kwargs):
    global LOCK
    LOCK = asyncio.Lock()
    await config.load_incluster_config()
    logger.info(f"Operator started in namespace {OPERATOR_NAMESPACE}")

    # Load Kubernetes client
    # custom_objects_api = client.CustomObjectsApi()
    with client.CustomObjectsApi() as api:
        omero_dropboxes = api.list_namespaced_custom_object(
            group="omero.lavlab.edu",
            version="v1",
            namespace=OPERATOR_NAMESPACE,
            plural="omerodropboxes"
        )
    for dropbox in omero_dropboxes.get('items', []):
        name = dropbox['metadata']['name']
        namespace = dropbox['metadata']['namespace']
        await reconcile_omerodropbox(name, namespace, dropbox['spec'], logger, **kwargs)

@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def reconcile_omerodropbox(name, namespace, spec, diff, logger, **_):
    """
    Reconcile the state of OmeroDropbox resources by ensuring the watch pod is in the desired state.
    """
    pod_name = f"{name}-watch"

    # Check if the pod exists
    with client.CoreV1Api() as api:
        try:
            pod = await api.read_namespaced_pod(name=pod_name, namespace=namespace)
            existing_pod_image = pod.spec.containers[0].image

            # Detect significant changes or if the pod's image is outdated
            significant_change_detected = any(
                op in ['add', 'change', 'remove'] and field_path[0] == 'spec'
                for op, field_path, _, _ in diff
            ) or existing_pod_image != OPERATOR_IMAGE

            if significant_change_detected:
                logger.info(f"Reconciling {pod_name} due to spec changes or outdated image.")
                # Delete and recreate the pod
                await api.delete_namespaced_pod(name=pod_name, namespace=namespace)
                logger.info(f"Deleted {pod_name} for recreation.")
            else:
                logger.info(f"{pod_name} already exists. Skipping creation.")
                return

        except ApiException as e:
            if e.status != 404:
                logger.error(f"Error checking existence of {pod_name}: {e}")
                return
        logger.info(f"{pod_name} does not exist. Creating...")
        await create_dropbox(spec, name, logger)
        logger.info(f"Recreated {pod_name} with updated configuration.")

@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def create_dropbox(spec, name, logger, **kwargs):
    logger.info(f"Creating OmeroDropbox {name} with spec: {spec}")
    pod_manifest = create_dropbox_pod_manifest(name, spec['watch'])
    await create_pod(pod_manifest, logger, name)

@kopf.on.delete('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def delete_omerodropbox(name, logger, **kwargs):
    logger.info(f"Deleting resources for OmeroDropbox {name}")

    with client.CoreV1Api() as api:
        pod_name = f"{name}-watch"
        try:
            await api.delete_namespaced_pod(pod_name, OPERATOR_NAMESPACE)
            logger.info(f"Pod {pod_name} deleted in namespace {OPERATOR_NAMESPACE}")
        except ApiException as e:
            if e.status == 404:  # Not found
                logger.info(f"Pod {pod_name} not found. It might have already been deleted.")
            else:
                logger.error(f"Failed to delete Pod {pod_name}: {e}")

@kopf.on.event ('batch', 'v1', 'jobs')
async def watch_jobs(namespace, logger, event, **kwargs):
    if event['type'] == 'DELETED':
        job_name = event['object']['metadata']['name']
        logger.info(f"Job {job_name} deleted in namespace {namespace}")
    else:
        job_status = event['object']['status'].get('conditions', [])
        for condition in job_status:
            if condition['type'] == 'Failed':
                job_name = event['object']['metadata']['name']
                logger.info(f"Job {job_name} failed in namespace {namespace}")
            elif condition['type'] == 'Complete':
                job_name = event['object']['metadata']['name']
                logger.info(f"Job {job_name} completed in namespace {namespace}")
            else:
                continue
            with client.BatchV1Api() as api:
                await api.delete_namespaced_job(job_name, namespace)
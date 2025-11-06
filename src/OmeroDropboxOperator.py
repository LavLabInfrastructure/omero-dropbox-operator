import os
import kopf
import yaml
import asyncio
# from kubernetes import client, config
from kubernetes_asyncio import client as client, config
from kubernetes_asyncio.client.rest import ApiException

# globals
OPERATOR_NAMESPACE = 'omero-dropbox-system'
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()


async def get_operator_image():
    pod_name = os.environ.get('HOSTNAME')
    async with client.ApiClient() as api_client:
        api = client.CoreV1Api(api_client)
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
    volumes, watch_manifest, webhook_manifest = None, None, None
    if 'pvc' in watch['watched']:
        watched_pvc_name = watch['watched']['pvc']['name']
        watched_pvc_path = watch['watched']['pvc'].get('path', '/')

        watch_manifest = _prepare_watch_container(OPERATOR_IMAGE, watched_pvc_path)
        webhook_manifest = _prepare_webhook_container(OPERATOR_IMAGE, name)

        volumes = [{
            "name": "watched-volume",
            "persistentVolumeClaim": {"claimName": watched_pvc_name}
        }]
    if volumes is None or watch_manifest is None or webhook_manifest is None:
        print('failed creating dropbox manifest')
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
    async with client.ApiClient() as api_client:
        api = client.CoreV1Api(api_client) 
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
    


@kopf.on.startup()
async def startup_fn(logger, **kwargs):
    global LOCK
    LOCK = asyncio.Lock()
    config.load_incluster_config()
    logger.info(f"Operator started in namespace {OPERATOR_NAMESPACE}")
    global OPERATOR_IMAGE
    OPERATOR_IMAGE = await get_operator_image()
    # Load Kubernetes client
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client)
        omero_dropboxes = await api.list_namespaced_custom_object(
            group="omero.lavlab.edu",
            version="v1",
            namespace=OPERATOR_NAMESPACE,
            plural="omerodropboxes"
        )
    for dropbox in omero_dropboxes.get('items', []):
        name = dropbox['metadata']['name']
        namespace = dropbox['metadata']['namespace']
        await reconcile_omerodropbox(name, namespace, dropbox['spec'], [], logger, **kwargs)

@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def reconcile_omerodropbox(name, namespace, spec, diff, logger, **_):
    """
    Reconcile the state of OmeroDropbox resources by ensuring the watch pod is in the desired state.
    """
    pod_name = f"{name}-watch"

    # Check if the pod exists
    async with client.ApiClient() as api_client:
        api = client.CoreV1Api(api_client)
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
                await wait_for_pod_deletion(api, pod_name, namespace, logger)

                # After deletion, recreate the pod with the updated configuration
                await create_dropbox(spec, name, logger)
                logger.info(f"Recreated {pod_name} with updated configuration.")

        except ApiException as e:
            if e.status == 404:
                # Pod does not exist, create it
                logger.info(f"{pod_name} does not exist. Creating...")
                await create_dropbox(spec, name, logger)
            else:
                logger.error(f"Error checking existence of {pod_name}: {e}")

async def wait_for_pod_deletion(api, pod_name, namespace, logger):
    """
    Wait for a pod to be deleted by polling its existence.
    """
    while True:
        try:
            await api.read_namespaced_pod(name=pod_name, namespace=namespace)
            logger.info(f"Waiting for pod {pod_name} to be deleted...")
            await asyncio.sleep(1)  # Sleep for a short time before retrying
        except ApiException as e:
            if e.status == 404:
                # Pod is deleted
                logger.info(f"Pod {pod_name} deleted successfully.")
                break
            else:
                logger.error(f"Failed to check pod deletion status: {e}")
                break

@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def create_dropbox(spec, name, logger, **kwargs):
    logger.info(f"Creating OmeroDropbox {name} with spec: {spec}")
    pod_manifest = create_dropbox_pod_manifest(name, spec['watch'])
    await create_pod(pod_manifest, logger, name)

@kopf.on.delete('omero.lavlab.edu', 'v1', 'omerodropboxes')
async def delete_omerodropbox(name, logger, **kwargs):
    logger.info(f"Deleting resources for OmeroDropbox {name}")

    async with client.ApiClient() as api_client:
        api = client.CoreV1Api(api_client)
        pod_name = f"{name}-watch"
        try:
            await api.delete_namespaced_pod(pod_name, OPERATOR_NAMESPACE)
            logger.info(f"Pod {pod_name} deleted in namespace {OPERATOR_NAMESPACE}")
        except ApiException as e:
            if e.status == 404:  # Not found
                logger.info(f"Pod {pod_name} not found. It might have already been deleted.")
            else:
                logger.error(f"Failed to delete Pod {pod_name}: {e}")


@kopf.on.event('batch', 'v1', 'jobs')
async def watch_jobs(namespace, logger, event, **kwargs):
    job_name = event['object']['metadata']['name']
    job_labels = event['object']['metadata'].get('labels', {})

    job_status = event['object']['status']
    condition_status = {cond['type']: cond['status'] for cond in job_status.get('conditions', [])}

    if event['type'] == 'DELETED':
        logger.info(f"Job {job_name} deleted in namespace {namespace}")
    elif condition_status.get('Complete') == 'True':
        logger.info(f"Job {job_name} completed successfully in namespace {namespace}")
        await cleanup_job(job_name, namespace, logger)
    elif condition_status.get('Failed') == 'True':
        logger.info(f"Job {job_name} failed in namespace {namespace}")
        await cleanup_job(job_name, namespace, logger)

async def cleanup_job(job_name, namespace, logger):
    async with client.BatchV1Api() as api:
        await api.delete_namespaced_job(job_name, namespace, propagation_policy='Background')
        logger.info(f"Cleaned up job {job_name} in namespace {namespace}")

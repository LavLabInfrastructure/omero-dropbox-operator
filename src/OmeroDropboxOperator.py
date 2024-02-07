import os
import kopf
import yaml
import asyncio
from kubernetes import client, config
from kubernetes.client.rest import ApiException

def get_operator_image():
    # Get the current pod name and namespace
    pod_name = os.environ.get('HOSTNAME')

    # Create an instance of the CoreV1Api
    core_v1_api = client.CoreV1Api()

    try:
        # Retrieve the pod information
        pod = core_v1_api.read_namespaced_pod(name=pod_name, namespace=OPERATOR_NAMESPACE)
        
        # Assuming the operator container is the first one, extract the image name
        operator_image = pod.spec.containers[0].image
        return operator_image
    except client.rest.ApiException as e:
        print(f"Exception when calling CoreV1Api->read_namespaced_pod: {e}")
        return None

def find_webhook_url():
    api_instance = client.CoreV1Api()

    try:
        # Use the correct label selector to find the webhook service
        services = api_instance.list_namespaced_service(namespace=OPERATOR_NAMESPACE, label_selector='app=omero-dropbox-operator-webhook')
        for svc in services.items:
            # Assuming the webhook service has a single, well-defined port
            port = svc.spec.ports[0].port if svc.spec.ports else 8080
            # Construct the webhook URL
            webhook_url = f"http://{svc.metadata.name}.{OPERATOR_NAMESPACE}.svc.cluster.local:{port}/import"
            return webhook_url
    except ApiException as e:
        print(f"Error fetching services in namespace {OPERATOR_NAMESPACE}: {e}")
    return None

def merge_lists(list1, list2):
    # Create a new list that starts with all items from list1
    merged_list = list1.copy()

    # Keep track of names already in the first list
    names_in_list1 = set(item['name'] for item in list1)

    # Add items from the second list if their names are not already in the first list
    for item in list2:
        if item['name'] not in names_in_list1:
            merged_list.append(item)

    return merged_list

def prepare_volumes_and_mounts(spec):
    volumes = [{
        "name": "watched-volume",
        "persistentVolumeClaim": {"claimName": spec['watch']['watched']['pvc']['name']}
    }]
    volume_mounts = [{"name": "watched-volume", "mountPath": "/watch"}]
    
    # Handle additionalMounts
    for mount in spec.get('watch', {}).get('spec', {}).get('additionalMounts', []):
        volumes.append({
            "name": mount['name'],
            "persistentVolumeClaim": {"claimName": mount['pvcName']}
        })
        volume_mounts.append({
            "name": mount['name'],
            "mountPath": mount['mountPath']
        })
    return volumes, volume_mounts

def create_watch_pod_manifest(name, env, volumes, volume_mounts):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{name}-watch", "namespace": OPERATOR_NAMESPACE},
        "spec": {
            "serviceAccountName": service_account_name,
            "containers": [{
                "name": "watch",
                "image": OPERATOR_IMAGE,
                "env": env,
                "volumeMounts": volume_mounts
            }],
            "volumes": volumes,
            "restartPolicy": "OnFailure"
        }
    }

def create_pod(pod_manifest, logger, name):
    pod_name = f"{name}-watch"
    namespace = OPERATOR_NAMESPACE
    api_instance = client.CoreV1Api()
    try:
        api_instance.read_namespaced_pod(name=pod_name, namespace=namespace)
        logger.info(f"Pod {pod_name} already exists in namespace {namespace}. Skipping creation.")
    except ApiException as e:
        if e.status == 404:  # Not found, safe to create
            try:
                api_instance.create_namespaced_pod(body=pod_manifest, namespace=namespace)
                logger.info(f"Pod {pod_name} created in namespace {namespace}.")
            except ApiException as create_error:
                logger.error(f"Failed to create Pod {pod_name}: {create_error}")
        else:
            logger.error(f"Failed to check existence of Pod {pod_name}: {e}")

# Load Kubernetes in-cluster configuration
config.load_incluster_config()

# Namespace where the operator is running, read from the mounted service account secret
OPERATOR_NAMESPACE = 'omero-dropbox-system'
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()

OPERATOR_IMAGE = get_operator_image()

# webhook_url = find_webhook_url()
#TODO proper webhook parsing
webhook_url = 'http://omero-dropbox-webhook.omero-dropbox-system.svc.cluster.local/import'

#TODO proper service account name parsing
service_account_name = 'omero-dropbox-webhook'

@kopf.on.startup()
async def startup_fn(logger, **kwargs):
    global LOCK
    LOCK = asyncio.Lock()
    logger.info(f"Operator started in namespace {OPERATOR_NAMESPACE}")

    # Load Kubernetes client
    custom_objects_api = client.CustomObjectsApi()
    omero_dropboxes = custom_objects_api.list_namespaced_custom_object(
        group="omero.lavlab.edu",
        version="v1",
        namespace=OPERATOR_NAMESPACE,
        plural="omerodropboxes"
    )

    for dropbox in omero_dropboxes.get('items', []):
        name = dropbox['metadata']['name']
        namespace = dropbox['metadata']['namespace']
        reconcile_omerodropbox(name=name, namespace=namespace, logger=logger)


@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
def reconcile_omerodropbox(name, namespace, spec, diff, logger, **_):
    """
    Reconcile the state of OmeroDropbox resources by ensuring the watch pod is in the desired state.
    """
    # Detect significant changes excluding metadata like labels or annotations
    significant_change_detected = any(
        op in ['add', 'change', 'remove'] and
        field_path[0] != 'metadata'
        for op, field_path, _, _ in diff
    )

    if significant_change_detected:
        logger.info(f"Significant change detected for {name} in namespace {namespace}, reconciling...")

        # Ensure Kubernetes client is configured
        config.load_incluster_config()
        api_instance = client.CoreV1Api()

        pod_name = f"{name}-watch"

        # Attempt to delete the existing watch pod if it exists
        try:
            api_instance.delete_namespaced_pod(name=pod_name, namespace=namespace)
            logger.info(f"Deleted existing watch pod {pod_name} in namespace {namespace}")
        except ApiException as e:
            if e.status != 404:  # Ignore error if the pod doesn't exist
                logger.error(f"Failed to delete pod {pod_name} in namespace {namespace}: {e}")
                return
            logger.info(f"Watch pod {pod_name} does not exist, nothing to delete.")

        # Recreate the watch pod with updated configuration
        # Assuming create_watch_pod is a function that creates the pod manifest based on the OmeroDropbox spec
        # and applies it to the cluster.
        create_dropbox(spec, name, logger)
        logger.info(f"Recreated watch pod {pod_name} in namespace {namespace}.")


@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
def create_dropbox(spec, name, logger, **kwargs):
    logger.info(f"Creating OmeroDropbox {name} with spec: {spec}")
    dropbox_cr = spec.get('watch', {})

    env = merge_lists(dropbox_cr.get('spec', {}).get('env', []), [
        {'name': 'MODE', 'value': 'WATCH'},
        {'name': 'WATCHED_DIR', 'value': f"/watch{spec['watch']['watched']['pvc']['path']}"},
        {'name': 'WATCH_NAME', 'value': name},
        {'name': 'WEBHOOK_URL', 'value': webhook_url}
    ])
    volumes, volume_mounts = prepare_volumes_and_mounts(spec)
    pod_manifest = create_watch_pod_manifest(name, env, volumes, volume_mounts)
    create_pod(pod_manifest, logger, name)

@kopf.on.delete('omero.lavlab.edu', 'v1', 'omerodropboxes')
def delete_omerodropbox(name, logger, **kwargs):
    logger.info(f"Deleting resources for OmeroDropbox {name}")

    api_instance = client.CoreV1Api()
    pod_name = f"{name}-watch"
    try:
        api_instance.delete_namespaced_pod(pod_name, OPERATOR_NAMESPACE)
        logger.info(f"Pod {pod_name} deleted in namespace {OPERATOR_NAMESPACE}")
    except ApiException as e:
        if e.status == 404:  # Not found
            logger.info(f"Pod {pod_name} not found. It might have already been deleted.")
        else:
            logger.error(f"Failed to delete Pod {pod_name}: {e}")
import os
import kopf
import yaml
from kubernetes import client, config

# Load Kubernetes in-cluster configuration
config.load_incluster_config()

# Namespace where the operator is running, read from the mounted service account secret
OPERATOR_NAMESPACE = 'omero-dropbox-system'
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()


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

OPERATOR_IMAGE = get_operator_image()

def find_webhook_url(namespace=OPERATOR_NAMESPACE):
    try:
        services = api_instance.list_namespaced_service(namespace=namespace, label_selector='role=import-webhook')
        for svc in services.items:
            # Assuming the webhook service has a single, well-known port
            port = svc.spec.ports[0].port if svc.spec.ports else 8080
            # Construct the service URL
            webhook_url = f"http://{svc.metadata.name}.{namespace}.svc.cluster.local:{port}/import"
            return webhook_url
    except ApiException as e:
        print(f"Error fetching services in namespace {namespace}: {e}")
    return None
    
@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
def handle_omerodropbox(spec, name, logger, **kwargs):
    logger.info(f"Handling OmeroDropbox {name} creation/update")
    final_spec = spec.get('watch', {}).get('spec', {})

    webhook_url = find_webhook_url()
    
    # Prepare environment variables
    env = final_spec.get('env', [])
    env.extend([
        {'name': 'MODE', 'value': 'WATCH'},
        {'name': 'WATCHED_DIR', 'value': f"/watch{spec['watch']['watched']['pvc']['path']}"},
        {'name': 'WATCH_NAME', 'value': name},
        {'name': 'WEBHOOK_URL', 'value': webhook_url}
    ])

    # Prepare volumes and mounts
    volumes, volume_mounts = prepare_volumes_and_mounts(spec, final_spec, logger)

    # Create Pod manifest
    pod_manifest = create_pod_manifest(name, final_spec, env, volumes, volume_mounts)

    # Create the Pod in Kubernetes, checking if it already exists
    create_pod(pod_manifest, logger, name)

@kopf.on.delete('omero.lavlab.edu', 'v1', 'omerodropboxes')
def delete_omerodropbox(name, logger=None, **kwargs):
    if logger is None:
        logger = logging.getLogger('kopf')
    logger.info(f"Deleting resources for OmeroDropbox {name}")

    pod_name = f"{name}-watch"
    try:
        api_instance.delete_namespaced_pod(pod_name, OPERATOR_NAMESPACE)
        logger.info(f"Pod {pod_name} deleted in namespace {OPERATOR_NAMESPACE}")
    except ApiException as e:
        if e.status == 404:  # Not found
            logger.info(f"Pod {pod_name} not found. It might have already been deleted.")
        else:
            logger.error(f"Failed to delete Pod {pod_name}: {e}")

def create_pod(pod_manifest, logger, name):
    pod_name = f"{name}-watch"
    namespace = OPERATOR_NAMESPACE
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

def prepare_volumes_and_mounts(spec, final_spec, logger):
    volumes = [{
        "name": "watched-volume",
        "persistentVolumeClaim": {"claimName": spec['watch']['watched']['pvc']['name']}
    }]
    volume_mounts = [{"name": "watched-volume", "mountPath": "/watch"}]
    
    # Handle additionalMounts
    for mount in final_spec.get('additionalMounts', []):
        volumes.append({
            "name": mount['name'],
            "persistentVolumeClaim": {"claimName": mount['pvcName']}
        })
        volume_mounts.append({
            "name": mount['name'],
            "mountPath": mount['mountPath']
        })
    return volumes, volume_mounts

def create_pod_manifest(name, final_spec, env, volumes, volume_mounts):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{name}-watch", "namespace": OPERATOR_NAMESPACE},
        "spec": {
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

def create_pod(pod_manifest, logger):
    api_instance = client.CoreV1Api()
    try:
        api_instance.create_namespaced_pod(OPERATOR_NAMESPACE, body=pod_manifest)
        logger.info(f"Pod {pod_manifest['metadata']['name']} created in namespace {OPERATOR_NAMESPACE}")
    except client.exceptions.ApiException as e:
        logger.error(f"Failed to create Pod {pod_manifest['metadata']['name']}: {e}")

import kopf
import yaml
from kubernetes import client, config

# Load Kubernetes in-cluster configuration
config.load_incluster_config()

# Namespace where the operator is running, read from the mounted service account secret
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()

def get_default_config(logger):
    core_v1_api = client.CoreV1Api()
    try:
        config_map = core_v1_api.read_namespaced_config_map("default-dropbox-config", OPERATOR_NAMESPACE)
        return yaml.safe_load(config_map.data['config'])
    except client.exceptions.ApiException as e:
        logger.error(f"Failed to read default config: {e}")
        return {}  # Return an empty config if there's an error

@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
def handle_omerodropbox(spec, name, logger, **kwargs):
    logger.info(f"Handling OmeroDropbox {name} creation/update")
    default_config = get_default_config(logger)
    watch_spec = spec.get('watch', {}).get('spec', {})
    
    # Merge defaults with overrides
    final_spec = {**default_config, **watch_spec}

    # Prepare environment variables
    env = final_spec.get('env', [])
    env.extend([
        {'name': 'WATCHED_DIR', 'value': f"/watch{spec['watch']['watched']['pvc']['path']}"},
        {'name': 'WATCH_NAME', 'value': name},
        {'name': 'WEBHOOK_URL', 'value': 'http://import-webhook.omero-dropbox-system.svc.cluster.local:8080/import'}
    ])
    
    # Prepare volumes and mounts
    volumes, volume_mounts = prepare_volumes_and_mounts(spec, final_spec, logger)
    
    # Create Pod manifest
    pod_manifest = create_pod_manifest(name, final_spec, env, volumes, volume_mounts)
    
    # Create the Pod in Kubernetes
    create_pod(pod_manifest, logger)

@kopf.on.delete('omero.lavlab.edu', 'v1', 'omerodropboxes')
def delete_omerodropbox(name, **kwargs):
    logger = kwargs.get('logger')
    logger.info(f"Deleting resources for OmeroDropbox {name}")

    # Example: delete a Pod created for the OmeroDropbox
    api_instance = client.CoreV1Api()
    pod_name = f"{name}-watch"
    try:
        api_instance.delete_namespaced_pod(pod_name, OPERATOR_NAMESPACE)
        logger.info(f"Pod {pod_name} deleted in namespace {OPERATOR_NAMESPACE}")
    except client.exceptions.ApiException as e:
        logger.error(f"Failed to delete Pod {pod_name}: {e}")

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
                "image": final_spec['image'],
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

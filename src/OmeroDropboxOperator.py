import kopf
import yaml
from kubernetes import client, config
config.load_incluster_config()
OPERATOR_NAMESPACE = "omero-dropbox-system"
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    OPERATOR_NAMESPACE = f.read().strip()

def get_default_config():
    core_v1_api = client.CoreV1Api()
    config_map = core_v1_api.read_namespaced_config_map("default-dropbox-config", OPERATOR_NAMESPACE)
    return yaml.safe_load(config_map.data['config'])

@kopf.on.create('omero.lavlab.edu', 'v1', 'omerodropboxes')
@kopf.on.update('omero.lavlab.edu', 'v1', 'omerodropboxes')
def handle_omerodropbox(spec, name, **kwargs):
    default_config = get_default_config()
    watch_spec = spec.get('watch', {}).get('spec', {})
    
    # Merge defaults with overrides
    final_spec = {**default_config, **watch_spec}
    
    # Ensure command is a list
    if not isinstance(final_spec['command'], list):
        final_spec['command'] = [final_spec['command']]
    
    # Prepare environment variables, adding WATCHED_DIR and WEBHOOK_URL
    env = final_spec.get('env', [])
    env.append({'name': 'WATCHED_DIR', 'value': f"/watch{spec['watch']['watched']['pvc']['path']}"})
    env.append({'name': 'WEBHOOK_URL', 'value': 'http://import-webhook.omero-dropbox-system.svc.cluster.local:8080/import'})  # Adjust as necessary
    
    volumes = [{
        "name": "watched-volume",
        "persistentVolumeClaim": {"claimName": spec['watch']['watched']['pvc']['name']}
    }]
    
    volume_mounts = [{
        "name": "watched-volume",
        "mountPath": "/watch"
    }]
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
    
    # Create a Pod manifest
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{name}-watch", "namespace": OPERATOR_NAMESPACE},
        "spec": {
            "containers": [{
                "name": "watch",
                "image": final_spec['image'],
                "command": final_spec['command'],
                "env": env,
                "volumeMounts": volume_mounts
            }],
            "volumes": volumes,
            "restartPolicy": "OnFailure"
        }
    }
    
    # Create the Pod in Kubernetes
    api_instance = client.CoreV1Api()
    api_instance.create_namespaced_pod(OPERATOR_NAMESPACE, body=pod_manifest)
    print(f"Pod {name}-watch created in namespace {OPERATOR_NAMESPACE}")

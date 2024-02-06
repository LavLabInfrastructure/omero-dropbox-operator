import yaml

from kubernetes import client, config
config.load_incluster_config()

from flask import Flask, request, jsonify
app = Flask(__name__)

# Load namespace
with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'r') as f:
    namespace = f.read().strip()

def get_config_map(namespace, name):
    core_v1 = client.CoreV1Api()
    config_map = core_v1.read_namespaced_config_map(name, namespace)
    return yaml.safe_load(config_map.data['config.yml'])  # Assuming the YAML is under 'config' key

def get_omero_dropbox_crd(namespace, name):
    custom_api = client.CustomObjectsApi()
    group = 'omero.lavlab.edu'
    version = 'v1'
    plural = 'omerodropboxes'
    omero_dropbox = custom_api.get_namespaced_custom_object(group, version, namespace, plural, name)
    return omero_dropbox

def overwrite_defaults(defaults, specifics):
    for key, value in specifics.items():
        if key in defaults and isinstance(defaults[key], dict) and isinstance(value, dict):
            overwrite_defaults(defaults[key], value)
        else:
            defaults[key] = value
    return defaults

def create_job(namespace, job_config, pvc_name, mount_path, work_path):
    volumes, volume_mounts = [{"name": "work-volume", "persistentVolumeClaim": {"claimName": pvc_name}}], [{"name": "work-volume", "mountPath": mount_path}]
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"generateName": "import-job-"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "worker",
                        "image": job_config['image'],
                        "command": job_config['command'] + [work_path],
                        "env": [{"name": k, "value": v} for k, v in job_config.get('env', {}).items()],
                        "volumeMounts": volume_mounts
                    }],
                    "volumes": volumes,
                    "restartPolicy": "Never"
                }
            }
        }
    }
    batch_v1 = client.BatchV1Api()
    job = batch_v1.create_namespaced_job(body=job_spec, namespace=namespace)
    return job.metadata.name


@app.route('/import', methods=['POST'])
def import_handler():
    data = request.json
    omero_dropbox_name = data['OmeroDropbox']
    full_path = data['fullPath']
    
    default_config_map = get_config_map(namespace, 'default-import-config')
    omero_dropbox_crd = get_omero_dropbox_crd(namespace, omero_dropbox_name)
    
    specific_config_map_name = omero_dropbox_crd.get('spec', {}).get('configMapName')
    specific_config_map = get_config_map(namespace, specific_config_map_name) if specific_config_map_name else {}
    
    job_config = overwrite_defaults(default_config_map, specific_config_map)
    pvc_name = omero_dropbox_crd.get('spec', {}).get('pvcName')
    work_path_in_pod = full_path  # Assume transformation to pod's path is handled if necessary
    
    job_name = create_job(namespace, job_config, pvc_name, job_config['mountPath'], work_path_in_pod)
    
    return jsonify({"message": "Job created successfully", "jobName": job_name}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
# Airflow Setup on Kind Cluster

Most steps are idempotent and interchangeable - this can be treated as more of a checklist than as a strict sequence of steps. Once your k8s cluster is up and running, there are lots of ways to provide new images and configuration to Airflow. 

> [!IMPORTANT]  
> This setup is useful for testing DAGs and Airflow components, but isn't a replica of our current cloud environment. This is intended to be a more agile, lower-effort way to test and develop DAGs locally, while also being able to shim in additional services using Helm and Kind. It's also a neat "getting started with kubernetes" exercise.

For MWAA-specific testing, consider https://github.com/aws/aws-mwaa-local-runner

## Prerequisites

- Docker
- Kind
- Helm 3.0+
- kubectl
- kubens
- kubectx
- (Optional) k9s


### Installing Docker

1. **Download Docker:**
   - Go to the [Docker download page](https://www.docker.com/products/docker-desktop) and download the installer for your operating system.

2. **Install Docker:**
   - Follow the installation instructions specific to your operating system.

3. **Verify Docker Installation:**
   ```bash
   docker --version
   ```

### Installing kube{x} utilities

1. kubectx bundles kubens and kubectl, so follow the instructions for your platform: https://github.com/ahmetb/kubectx?tab=readme-ov-file#installation

### Installing Kind

1. Install `kind` using instructions for your local operating system: https://kind.sigs.k8s.io/docs/user/quick-start/

2. Verify Kind Installation:

```bash
    kind --version
```

### Installing Helm

**1. Download Helm Installation Script:**

```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
```

This method gets the most up-to-date installation (I've had issues with the `apt` package being outdated in the past). There are other methods that are untested, such as `brew install helm`

**2. Verify Helm Installation:**

```bash
    helm version
```

### Setting Up Airflow on Kind Cluster
**1. Create Kind Cluster**

```bash
kind create cluster --name airflow-cluster
```

If this is not your first k8s rodeo, `kubectx airflow-cluster` will switch your context to the new cluster.

**2. Build and tag your Docker image:**

Assuming you are in the root directory of the project:
```bash
docker build -f ./local-setup/Dockerfile.airflow -t veda-airflow .
```

**3. Load the Docker image to Kind:**

```bash
kind load docker-image veda-airflow:latest --name airflow-cluster
```

**4. Add Airflow Helm Repo:**

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

**5. Create Kubernetes Namespace:**

```bash
kubectl create namespace airflow
```

**6. Install Airflow:**

```bash
helm install airflow apache-airflow/airflow --namespace airflow -f ./local-setup/airflow-values.yaml
```

**7. Verify Pods are Running:**

```bash
kubectl get pods --namespace airflow
```

Alternatively, monitor the deployment using k9s:

```bash
k9s --namespace airflow
```

**8. Port Forward to Airflow Webserver:**

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

Now you should be able to access the Airflow UI at `http://localhost:8080` 🎉

### Updating Airflow Image

**1. Build and tag your updated Docker image:**

```bash
docker build -f ./local-setup/Dockerfile.airflow -t veda-airflow .
```
**2. Load the updated image to Kind:**

```bash
kind load docker-image veda-airflow:latest --name airflow-cluster
```

**3. Update the Helm installation to use the new image (optional shortcut):**

```bash
helm upgrade airflow apache-airflow/airflow --namespace airflow --set images.airflow.tag=latest
```

## Troubleshooting

If you are using `Airflow < 2.7`, you need to add `--version 1.10.0` to your `helm install` and `helm upgrade` commands.

If you are missing "Variables" and your DAGs fail to import as a result, you can add new Variables in the Airflow Admin UI. For reference, a template is available at `infrastructure/mwaa_environment_variables.tpl`, and a complete file is present at `/tmp/mwaa_vars.json` if you have deployed from your local machine before.

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

2. Verify kubectx Installation:

```bash
kubectx --help
```

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
kubectx airflow-cluster
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

Optional: Verify the image is loaded:

```bash
docker exec -it airflow-cluster-control-plane bash
# in docker terminal
crictl images
# veda-airflow should be listed, along with other system images
```

**4. Add Airflow Helm Repo:**

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

**5. Create Kubernetes Namespace:**

```bash
kubectl create namespace airflow
kubens airflow
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

- If you are using `Airflow < 2.7`, you need to add `--version 1.10.0` to your `helm install` and `helm upgrade` commands.

- If you are missing "Variables" and your DAGs fail to import as a result, you can add new Variables in the Airflow Admin UI. For reference, a template is available at `infrastructure/mwaa_environment_variables.tpl`, and a complete file is present at `/tmp/mwaa_vars.json` if you have deployed from your local machine before.

- If you're stuck on AWS permissions when running DAGs - https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html - you can specify a role to assume, or simply use your own credentials (this gets me 99% of the way, by allowing me to access resources on UAH)

## k8s Troubleshooting

- If helm is timing out with an error message about migrations, you can try to increase the timeout:

    ```bash
    helm install airflow apache-airflow/airflow --namespace airflow -f ./local-setup/airflow-values.yaml --timeout 10m
    ```
    Alternatively, increase the `images.migrationsWaitTimeout` value in `airflow-values.yaml`

- If helm installs are still failing, there are several possible reasons. To help identify the issue, you can use `k9s` to investigate the state of the cluster.

    - Without k9s, you can still access the same information using `kubectl` commands.

        ```bash
        kubectl get pods,svc,deployments --namespace airflow
        # identify the pods, services, or deployments that are failing or stuck in a loop
        kubectl logs pod/<pod-name> --namespace airflow # or svc/<service-name>, deployment/<deployment-name> etc
        kubectl describe pod/<pod-name> --namespace airflow
        ```

- Fixing a broken state after timeouts (during helm upgrades or installs)
    ```bash
    kubens airflow
    helm uninstall airflow
    ```


- `Error: secret "airflow-metadata" not found`

    This is likely a symptom of installing a new version, followed by the old one. Run the `helm uninstall` command above to remove the old version.

- Out of space on postgres pod
    ```
    │ postgresql 22:51:11.57 INFO  ==> ** Starting PostgreSQL **                                                            │
    │ 2024-07-11 22:51:11.589 GMT [1] FATAL:  could not write lock file "postmaster.pid": No space left on device           │
    │ Stream closed EOF for airflow/airflow-postgresql-0 
    ```
    This is a known issue with the postgres container, and can be caused by Docker engine settings, or by an improperly removed PVC. The easiest way to fix this is to uninstall/reinstall the helm chart with the added step of removing the PVCs:
    ```
    kubectl delete pvc/data-airflow-postgresql-0
    ```


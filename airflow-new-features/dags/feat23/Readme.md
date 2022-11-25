# Airflow 2.3 new features

This subdirectory covers some features introduced in Airflow
2.3. 

These include:
- [ ] Dynamic Task Mapping
- [x] Tree view replaced by Grid view (demonstrated by default)
- [x] LocalKubernetesExecutor
- [ ] Reuse of decorated tasks

The DAG local_kubernetes_executor is excluded from
running with the other examples since it requires
a local kubernetes cluster. To run the example, read
the section LocalKubernetesExecutor and setup a locak
kubernetes cluster. 

# LocalKubernetesExecutor

The `LocalKubernetesExecutor` allows the user to 
simultaneously run both the local and kubernetes executors.
Airflow will instantiate both executors and the task
will run on one of them. The executor is chosen based on 
the task's queue. 

However, to run the kubernetes executor, we need to setup
a local kubernetes cluster.

## Running the example
First make sure that you kave kubectl, helm and kind 
installed. We'll use all three to setup the kubernetes
cluster. 

First create the cluster using the following command:
```shell
kind create cluster -n airflow
```
This will create our cluster named airflow.

Next crate the namespace and set the correct kubernetes
context:
```shell
kubectl create namespace airflow && kubectl config set-context --current --namespace airflow
```

Check the context:
```shell
kubectl config get-contexts | grep kind-airflow
```
and you should see something like this:
```shell
*   kind-airflow    kind-airflow    kind-airflow    airflow
```

Going forward, at any point if you need to investigate
the pods, you can use any of the following commands:
```shell
kubectl get pods
kubectl describe pod <pod-name>
kubectl logs <pod-name>`
```

Now, let's first setup our remote storage. For it we will
use minio. Run:
```shell
kubectl apply -f k8s/minio-dev.yaml   
```
This will bring up a pod for minio. 

Activate port forwarding:
```shell
kubectl port-forward pod/minio 9000 9090
```

You can now check that it is running by going to 
`localhost:9000` and logging in with the default 
credentials:
- user: minioadmin 
- pass: minioadmin

We need to setup the bucket for the logs in minio. 
Open a new terminal. Now run minio mc from a docker 
container: 
```shell
docker run -it \
    --rm \
    --network host \
    --add-host host.docker.internal:host-gateway \
    --mount type=bind,source=$(pwd)/k8s/prep-logging.sh,target=/prep-logging.sh \
    --entrypoint="/bin/sh" \
    minio/mc /prep-logging.sh
```

You should get the following output if everything was ok:
```shell
mc: Configuration written to `/root/.mc/config.json`. Please update your access credentials.
mc: Successfully created `/root/.mc/share`.
mc: Initialized share uploads `/root/.mc/share/uploads.json` file.
mc: Initialized share downloads `/root/.mc/share/downloads.json` file.
Added `minio-airflow` successfully.
Bucket created successfully `minio-airflow/airflow-logs`.
```

If you navigate to `localhost:9000`, once you login,
you should see the bucket named airflow-logs.

![minio remote logging](../../resources/minio-remote-logging.png)

Ok, this was the easy part.

Now, let's bring up Airflow. First, we need to create
a secret for syncing with the repo on GitHub.
If you want, create a new RSA key:
```shell
ssh-keygen -t rsa -b 4096 -C "<john.doe@gmail.com>"
```
And paste the key to your GitHub account.

Now, we'll create a kubernetes secret with this key
(check the path for your computer):
```shell
kubectl create secret generic airflow-git-secret --from-file=/Users/<username>/.ssh/id_rsa.pub --namespace airflow
```
Next, we'll need a service account named `airflow-worker`.
For some reason, in my use-case, the helm chart doesn't
create one. So, I needed to do this:
```shell
kubectl apply -f k8s/service-accounts.yaml --namespace airflow
```

If you execute the following commands, you should see
the created secret and service accounts:
```shell
kubectl get secrets
kubectl get serviceaccount
```

Add the connection that we will use for remote logging:
```shell
kubectl create secret generic minio --from-literal=conn="s3://minioadmin:minioadmin@?host=http%3A%2F%2Fminio-service%3A9000"
```

Ok, now for the fun part. First, you need to add the 
helm chart repo to your local helm configuration:
```shell
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

Now, install the helm chart using the following command:
```shell
helm upgrade --install airflow apache-airflow/airflow -n airflow -f k8s/helm-override.yaml --debug
```
This might take some time. Wait for it to finish
before continuing.

Check that all the pods are running using:
```shell
kubectl get pods
```
You should get something like this at this point:
```shell
NAME                                 READY   STATUS    RESTARTS   AGE
airflow-postgresql-0                 1/1     Running   0          77s
airflow-scheduler-0                  3/3     Running   0          76s
airflow-webserver-77cbf55486-gbc2k   1/1     Running   0          77s
minio                                1/1     Running   0          25m
```

And let's expose the webserver using port-forwarding:
```shell
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
```

You should now be able to go to `localhost:8080` and
turn on the `local_kubernetes_executor_dag`.

The DAG should run at least 3 times upon start.
Here is the view of the UI I had after the DAG finished
a few times:
![log_view_webui](../../resources/log_view_webui.png)


Looking at the `print_task_with_local` task, you can see 
this log:
![log_task_with_local](../../resources/log_view_small_local.png)

Also, to confirm that it was actually executed on
the local executor, find the line marked with "**":
```shell
[2022-11-25, 09:20:41 UTC] {taskinstance.py:1363} INFO - Starting attempt 1 of 1
[2022-11-25, 09:20:41 UTC] {taskinstance.py:1364} INFO - 
--------------------------------------------------------------------------------
[2022-11-25, 09:20:41 UTC] {taskinstance.py:1383} INFO - Executing <Task(PythonOperator): print_task_with_local> on 2022-11-25 09:15:40.216812+00:00
[2022-11-25, 09:20:41 UTC] {standard_task_runner.py:54} INFO - Started process 326 to run task
[2022-11-25, 09:20:41 UTC] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'local_kubernetes_executor', 'print_task_with_local', 'scheduled__2022-11-25T09:15:40.216812+00:00', '--job-id', '15', '--raw', '--subdir', 'DAGS_FOLDER/local_kubernetes_executor_dag.py', '--cfg-path', '/tmp/tmpk54gsx51']
[2022-11-25, 09:20:41 UTC] {standard_task_runner.py:83} INFO - Job 15: Subtask print_task_with_local
[2022-11-25, 09:20:41 UTC] {dagbag.py:525} INFO - Filling up the DagBag from /opt/airflow/dags/repo/airflow-new-features/dags/feat23/dags/local_kubernetes_executor_dag.py
**[2022-11-25, 09:20:41 UTC] {task_command.py:384} INFO - Running <TaskInstance: local_kubernetes_executor.print_task_with_local scheduled__2022-11-25T09:15:40.216812+00:00 [running]> on host airflow-scheduler-0.airflow-scheduler.airflow.svc.cluster.local**
[2022-11-25, 09:20:42 UTC] {taskinstance.py:1592} INFO - Exporting the following env vars:
```

You should see something like: 
```shell
[running]> on host airflow-scheduler-0.airflow-scheduler.airflow.svc.cluster.local
```

Looking at the `print_task_with_kubernetes` task, you can
see this log:
![log_task_with_kubernetes](../../resources/log_view_small_kubernetes.png)

To confirm that the task was actually executed on
the kubernetes executor, find the line marked with "**":
```shell
[2022-11-25, 09:22:28 UTC] {taskinstance.py:1363} INFO - Starting attempt 1 of 1
[2022-11-25, 09:22:28 UTC] {taskinstance.py:1364} INFO - 
--------------------------------------------------------------------------------
[2022-11-25, 09:22:28 UTC] {taskinstance.py:1383} INFO - Executing <Task(PythonOperator): print_task_with_kubernetes> on 2022-11-25 09:15:40.216812+00:00
[2022-11-25, 09:22:28 UTC] {standard_task_runner.py:54} INFO - Started process 20 to run task
[2022-11-25, 09:22:28 UTC] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'local_kubernetes_executor', 'print_task_with_kubernetes', 'scheduled__2022-11-25T09:15:40.216812+00:00', '--job-id', '16', '--raw', '--subdir', 'DAGS_FOLDER/local_kubernetes_executor_dag.py', '--cfg-path', '/tmp/tmp3c6bxmhj']
[2022-11-25, 09:22:28 UTC] {standard_task_runner.py:83} INFO - Job 16: Subtask print_task_with_kubernetes
[2022-11-25, 09:22:28 UTC] {dagbag.py:525} INFO - Filling up the DagBag from /opt/airflow/dags/repo/airflow-new-features/dags/feat23/dags/local_kubernetes_executor_dag.py
**[2022-11-25, 09:22:28 UTC] {task_command.py:384} INFO - Running <TaskInstance: local_kubernetes_executor.print_task_with_kubernetes scheduled__2022-11-25T09:15:40.216812+00:00 [running]> on host localkubernetesexecutorprintta-7130507b037e450ca3c8ad503742da61**
[2022-11-25, 09:22:28 UTC] {taskinstance.py:1592} INFO - Exporting the following env vars:
```
you should see something like: 
```shell
[running]> on host localkubernetesexecutorprintta-7130507b037e450ca3c8ad503742da61
```
![minio_dag_logging](../../resources/minio-dag-logging.png)

And the logs are inside:

![minio_log_files](../../resources/minio_log_files.png)

If you log in to minio s3 storage (presumably running
on `localhost:9000`), you should see the log files.

## Cleaning up

After you're finished with the example, you might
want to clean up the created resources. First, stop
any port-forwarding that you have running.

First, delete the helm chart installed:
```shell
helm delete airflow
```

This will delete most of the resources

Delete the rest with kubectl:
```shell
kubectl delete all --all
```
In my case, this leaves the service account and secrets.
So delete those separately:
```shell
kubectl delete secret --all
kubectl delete serviceaccount --all
```

Delete the cluster:
```shell
kind delete cluster -n airflow
```

## Why use remote logging? 
Well, to use kubernetes PV and PVC, you need to have
a PVC with the ReadWriteMany access mode. As far as I could
tell, the class storage that supports it, isn't 
pre-installed with kubectl. It seeemed to be more work
to setup such a PVC than to use remote logging. 
You can read more [here](https://airflow.apache.org/docs/helm-chart/1.7.0/manage-logs.html). 

If your PVC doesn't support this access mode, some pods 
will be in the Pending state indefinitely. 

# References
1. https://airflow.apache.org/docs/helm-chart/stable/quick-start.html
2. https://www.oak-tree.tech/blog/airflow-remote-logging-s3
3. https://airflow.apache.org/docs/apache-airflow/2.3.0/executor/local_kubernetes.html
4. https://airflow.apache.org/docs/helm-chart/1.7.0/manage-logs.html

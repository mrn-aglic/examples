# Airflow 2.2 new features

This subdirectory covers some features introduced in Airflow
2.2. 

These include:

## Custom timetables
The most relevant file here is CustomTimetable.py. 

# Deferrable operators and triggers


## What are deferrable operators? 

The short answer is that they are operators/sensors that 
can free up slots while they are waiting for some 
operation to finish. 
Workers are actually processes that execute the 
operator.

When a defferable operator is suspended, the work it needs
to do is handed off to a _trigger_.
Triggers are run by the _triggerer service_.

### So how does it work? 
A deferrable operator comes to a point where it has to 
wait. The operator defers itself using the `defer` method
which accepts a **trigger instance**. 
The defer method accepts a few parameters. The most 
important are the trigger instance that will continue 
the work and the **method_name** argument. The 
**method_name** argument specifies the method that should
be called when the trigger returns execution to the 
operator. 

Other than that, the defer method accepts kwargs (
default `{}`)and a timeout (default `None`) arguments. 

Once the operator is deferred, the trigger instance
is picked up by the triggerer process. Therefore, a 
triggerer process needs to be added alongside
other existing processes (workers, scheduler, webserver).

The triggerer process calls the async run method of the
trigger instance. The trigger instance will execute
until it yields a `TriggerEvent` instance or throws an 
exception. In the case of an exception, the operators 
that depend on the trigger instance are also failed. 

The trigger instance can yield one or more `TriggerEvent`
instances. Once `TriggerEvent` is yielded, the scheduler
queues the deferred operator so it can resume its work. 

The `TriggerEvent` instance accepts a payload that can
be accessed from the method called when the deferred
operator resumes work. 
Of course, since we are in a distributed setting, I **assume**
that this **payload should not be large**.

**IMPORTANT:** no state is persisted in a deferred operator.
The only way to pass state to a new instance of the operator
is via `kwargs` and `method_name`. Although, personally,
I fail to see how passing through `method_name` is useful.
You should also avoid having persistant state in the 
trigger. Everythin that the trigger needs should be passed
through the constructor.

### Summary
- Deferred operator: operator is instantiated -> 
defer method is called -> 
- Triggerer: execution is passed to async run of trigger -> 
trigger yields `TriggerEvent` -> 
- Deferred operator: the method defined by
`method_name` is executed

### The method_name method
The defer method, previously mentioned, accepts a
`method_name` argument. This arguments specifies the 
method that should be called when the deferred operators 
needs to resume execution. It is called once the 
`TriggerEvent` is fired. The method must accept 
the `event` keyword argument. Potential paylod from the 
`TriggerEvent` will be passed to this argument. 

The full definition of a method used for method_name is:
```python
def execute_complete(self, context: Context, event: Optional[dict] = None) -> None:
...
```

### The trigger implementation
The trigger needs to define:
1. `__init__` for the arguments that will be passed from
the operator;
2. run - async method that yields `TriggerEvent` instances
as an async generator;
3. serialize - used to reconstruct the trigger instance.

The class that defines the trigger needs to inherit from
`BaseTrigger`.

### The defferable operator implementation
The class that defines a deferable operator should inherit
from `BaseSensorOperator`. 
You should implement two methods in the deferrable operator:
1. `execute` that accepts context
2. whatever method you want to continue execution once
the deferred operator is resumed.


## The example
The first example I chose is a Http deferrable operator.
The full definition of all components from the operator
are located in the package `defferable`. The package is 
based on the Astronomer [HttpSensorAsync](https://registry.astronomer.io/providers/astronomer-providers/modules/httpsensorasync).
All operators from Astronomer's repo are licensed under 
Apache License 2.0 at the time of writing. 

There are 3 main components to the deferrable operator
(HttpSensorAsync): 
1. the operator itself
2. the operator's dependency - HttpTrigger
3. the trigger's dependency - HttpAsyncHook

Why number 3? Because defferable operators hsould be
async from top to bottom. 

### HttpAsyncHook
The hook implementation must be async. In this example,
the inherits from `BaseHook`. 

That means that the Hook also needs to be async for any
operation that might take some time.
The hook needs to have an async run function. 
In the example the function is invoked to request data 
from the API. The request is carried out using 
`aiohttp.ClientSession()`. 

`BaseHook` defines a `get_connection` method that queries
the secret backend. Therefore, the method to get the 
connection also needs to be async. If you look at the 
definition of `get_conn` in the hook, it passes the 
`get_connection` base method to the `sync_to_async` 
function. The `sync_to_async` function returns a `SyncToAsync`
instance. The key here is that the instance has an 
`async __call__` method defined which makes it a 
coroutine when called.

### HttpTrigger
The trigger's only async method is run that is executed
when the triggerer process picks up the trigger instance.

The serialize method needs to return the information
needed to create a new instance of the trigger. Hence,
we return a dictionary of the arguments passed to the 
`__init__` method and the classpath. 

Because of the classpath, the package needs to be 
discoverable by the triggerer. 

### HttpSensorAsync
HttpSensorAsync is the deferrable operator. It simply
implements the required methods and calls defer:
```python
def execute(self, context: Context) -> Any:
    self.defer(
        trigger=HttpTrigger(
            http_conn_id=self.http_conn_id,
            method=self.method,
            endpoint=self.endpoint,
            data=self.data,
            headers=self.headers,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
        ),
        method_name="execute_complete",
    )
```

The method that will be called when the operator resumes
is called `execute_method` in this case.

This operator should inherit from `BaseSensorOperator`.

## Important notes
From the Airflow documentation on module management, the 
modules should be organised as follows: 
```shell
<DIRECTORY ON PYTHONPATH>
| .airflowignore  -- only needed in ``dags`` folder, see below
| -- my_company
              | __init__.py
              | common_package
              |              |  __init__.py
              |              | common_module.py
              |              | subpackage
              |                         | __init__.py
              |                         | subpackaged_util_module.py
              |
              | my_custom_dags
                              | __init__.py
                              | my_dag1.py
                              | my_dag2.py
                              | base_dag.py
```

With this organisation, you can reference the package 
modules in your dag: 
```python
from feat22.mypkg.httpdeferrable import HttpSensorAsync
```
However, the most organised way of adding custom code
is to create a Python package and install it to a 
location that is on the Python path. 

[//]: # (However, this directory may not be on the python path.)

[//]: # (Why is it important to keep it on the python path?)

[//]: # (Because of the serialize method of the trigger.)

Ideally, the module that you install will have its own 
repo on GitHub and you would install it from the repo 
while building the Dockerfile. 

Both the scheduler and triggerer need to have the package
installed. 

**The logs** for the trigger instance (`HttpTrigger` in the 
example and its dependencies) **will show in the triggerer
service**, not the scheduler. Unless an exception is thrown
and the operator fails.

# References
1. https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html
2. https://registry.astronomer.io/providers/astronomer-providers/modules/httpsensorasync
3. https://airflow.apache.org/docs/apache-airflow/stable/modules_management.html

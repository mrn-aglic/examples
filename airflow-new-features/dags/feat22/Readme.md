# Airflow 2.2 new features

This subdirectory covers some features introduced in Airflow
2.2. 

These include:
- [x] custom timetables
- [x] deferrable operators

To run only Airflow 2.2 examples, run:
```shell
make run-2.2
```

# Custom timetables
The most relevant file here is `CustomTimetable.py`. 
The file is located in the plugins folder and the 
`CustomTimetable` defined is treated as a plugin. 

Timetables allow us to customize the schedule and interval
in which Airflow DAGs will run. The example given in
the Airflow docs describes a use-case in which we want 
a DAG to collect data from Monday to Friday. 

This could be achieved by setting the schedule interval:
```shell
schedule_interval="0 0 * * 1-5"
```
But, this means that for Friday, we will get the data on 
Monday. What a user may want is to process the data for
Friday on Saturday. Therefore, there would be **no DAG runs**
on **Sunday and Monday**.

To implement a custom timetatble, the class needs to 
inherit from `Timetable` and implement two abstract 
methods. Once you implemented the custom timetable, 
you need to register it as a plugin, which is done
in this example with:
```python
class CustomTimetablePlugin(AirflowPlugin):
    name = "custom_timetable"
    timetables = [CustomTimetable]
```

You can view the example in the airflow docs [here](https://airflow.apache.org/docs/apache-airflow/2.2.0/howto/timetable.html).
There is also an example from Astronomer [here](https://docs.astronomer.io/learn/scheduling-in-airflow).


The custom timetable that you define needs to implement
two methods:
1. `infer_manual_data_interval`
2. `next_dagrun_info` 

The first method determines how to infer the run interval
when the DAG is run manually. 
For example, out of schedule.

The scheduler uses the second method to determine the 
DAG's regular schedule. 

For this repo, and to try and better understand how 
timetables work, I changed the implementation of the 
example (found [here](https://airflow.apache.org/docs/apache-airflow/2.2.0/howto/timetable.html)) a bit. 
In this changed example I assume that we do not want to 
**execute** DAG runs between 13 PM and 17 PM.
Another way to say this is that we don't want to 
schedule any dag runs between 12 PM and 17 PM. But we
can **execute** one at 13 PM.

###Note 
When testing make sure that the webserver UI is set to
display time in UTC to make it easier to validate the
correctness of the code.
The timetable supports only UTC time at the moment.

## CustomTimetable in repo
Let's see how we implemented the CustomTimetable.

**NOTE** I didn't extensively test this custom timetable.
If you find a bug feel free to submit a pull request. 

We know that a DAG run occurs when the interval of the
run expires. In this case, the interval is 1 hour.
The approach is basically to **calculate
the start datetime for the interval**, and then simply
**add 1 hour to it for the end of the interval**.

We'll start with the simpler method first, i.e. 
`infer_manual_data_interval`.

### Infer manual data interval

The code:
```python
def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
    # run_after is the time when the DAG was externally triggered
    hour = run_after.hour

    if hour in self.dont_run:
        # if the hour is in the range 12-17, infer hours since interval start
        # e.g. 15 - 12 = 3
        hours_since_interval_star = hour - min(self.dont_run)
        delta = timedelta(hours=hours_since_interval_star)
    else:
        delta = timedelta(hours=1)

    # place the start of the run at 15 - 3 = 12
    start = (run_after - delta).replace(tzinfo=UTC)

    # Move the end by 1 hour
    return DataInterval(start=start, end=(start + timedelta(hours=1)))
```

When the DAG is manually triggered, the scheduler uses
this method to infer the out-of-schedule DAG run interval.
The method accepts the `run_after` argument which represents
The date and time of when the DAG run was manually 
triggered.

We first take the hour at which the manual trigger was 
clicked. If the hour is in the `dont_run` set, we push
back the start time. For example, if we triggered the
DAG at 15 hours, the `hours_since_interval_start` will be 
15 - min_of_set (which is 12) = 3.
The delta will also be 3. 

We then move the start to be current time - 3 hours = 
some time at the 12th hour.

Finally, we want the DAG's interval to be 1 hour. So, 
we set the end of the interval to start + 1 hour. 
The end of the interval will then be 13 hours.

**The webserver calls this method**. 

### Next dag run info

The other method is `next_dagrun_info`. This method is
used to infer the regular DAG run schedule. 
Here is the code:
```python
def next_dagrun_info(
    self,
    *,
    last_automated_data_interval: DataInterval | None,
    restriction: TimeRestriction,
) -> DagRunInfo | None:

    logging.info("next dagrun info executing")

    if last_automated_data_interval is not None:
        next_start = self._get_next_start_prev_run_exists(
            last_automated_data_interval
        )

    else:
        next_start = self._get_next_start_with_no_prev_run(restriction)

    # latest refers to the latest dag run that the dag can be scheduled, calculated from
    # end_date arguments
    if restriction.latest is not None and next_start > restriction.latest:
        return None  # Over the DAG's scheduled end; don't schedule.

    # Once we obtained the start of the next_run, setup the interval
    return DagRunInfo.interval(
        start=next_start, end=(next_start + timedelta(hours=1))
    )
```
There are two special cases that we need to handle:
1. whether it is the first ever dag run
2. whether there were previous dag runs.

I separated the logic into two methods to make the code
more readable (with comments). Let's first look at the case
when there are no previous dag runs:
```python
def _get_next_start_with_no_prev_run(
    self, restriction: TimeRestriction
) -> DateTime | None:

    # The next_start is the earliest possible
    # this is calculated from all the start_date arguments of the DAG
    # and its tasks. It is None if there are no start_date_arguments found
    next_start = restriction.earliest

    if next_start is None:  # No start_date. Don't schedule.
        return None

    # Get the current hour
    current_hour = Time(DateTime.now().hour)

    if not restriction.catchup:
        # If the DAG has catchup=False, today is the earliest to consider.
        next_start = max(
            next_start,
            DateTime.combine(Date.today(), current_hour).replace(tzinfo=UTC),
        )
    # not sure how useful this logic is
    # elif (
    #     next_start.date() == DateTime.today()
    #     and next_start.hour == current_hour
    #     and next_start.minute > 0
    # ):
    #     next_hour = Time(current_hour + 1)
    #     # If catchup is enabled and we are today at the same hour and the minute is not 0.
    #     # That is, the hour is, e.g. 18:01
    #     # Then skip to the next hour
    #     next_hour = Time(next_hour + 1)
    # 
    #     # combine the next start date (today) and the next hour
    #     next_start = DateTime.combine(next_start.date(), next_hour).replace(
    #         tzinfo=UTC
    #     )

    # check whether the next run hour is in the don't run range, i.e. 12-17
    is_hour_in_dont_run = next_start.hour in self.dont_run

    if (
        is_hour_in_dont_run
    ):  # If next start is in the hour not to run, go to next run hour.
        # e.g. if the hour is 12:00
        # 17 - 12 = 5
        next_run_hour = max(self.dont_run) - next_start.hour

        # the timedelta will be 5 hours in the future
        delta = timedelta(hours=next_run_hour)

        next_start = next_start + delta

    # return the next start date
    return next_start.set(minute=0, second=0, microsecond=0)
```

We take the next start as the earliest possible. Then
we check whether there is a start date. If there is
none, we simply return `None`.

Then, we check whether catchup is enabled. If not, we try
to determine the maximum start date between the earliest
possible registered and a combination of the current date
and hour. 
For example, the earliest start date could have been
January 1st 2022. But, catchup is set to `False`. Therefore,
the next dag run should be today, and not January 1st. 

If the next dag run is scheduled for today and the 
current time is 18:02, and the next start is 
scheduled at 18:00, you may want to move the next start 
to 19:00. I left that piece of code commented out
since it doesn't seem all that useful. 

At this point we have determined the standard hour at
which our DAG should run. 

Next, we need to check whether that hour falls into
the set of hours that we want to skip. If yes, calculate
a delta value. The delta value will push the next dag run
to skip these hours. For example, if its 12:00, we 
calculate 17 - 12 = 5. The delta is 5, and push the value
`next_start` value to be at 17 PM. **Basically, we tell
Airflow, the next start will be at 17:00.** 

Before returning the value we set the minutes, seconds
and microseconds to 0.

**IMPORTANT NOTE:** the first data interval is calculated
when the DAG is parsed. Therefore, the logs for that case
may be unavailable. What you can do is connect to the
postgres database and query the dag table. Find the dag
that you are interested in and check the columns:
- next_dagrun_interval_start
- next_dagrun_interval_end
- next_dagrun


The next case is when there exists a previous run:
```python
def _get_next_start_prev_run_exists(
    self, last_automated_data_interval: DataInterval | None
) -> DateTime:
    # this method is executed if this is not the first scheduled DAG run
    last_start = last_automated_data_interval.start

    # get the hour of the last run
    last_start_hour = last_start.hour

    if last_start_hour not in self.dont_run:
        # the hour of the last run is not in the range 12-17, e.g. 11
        # so the next run can be 1 hour later
        delta = timedelta(hours=1)
    else:
        # the hour of the last run is in the range 12-17, so we need to push the hour later
        # e.g. last_start_hour = 15. 17 - 15 = 2 -> the delta
        hour_delta = max(self.dont_run) - last_start_hour
    
        # The problem here with calculating the next DAG run is if the delta ends up being 0.
        # This can happen in the following case:
        # 1. The latest DAG is run at 17 UTC
        # 2. we get last_start_hour = 17. max(self.dont_run) - last_start_hour = 0
        # In this case the scheduler will enter an infinite loop since it will try to schedule
        # a DAG run at the same time as an existing one.
        # a simple fix is:
        if hour_delta == 0:
            hour_delta = 1
        delta = timedelta(hours=hour_delta)

    # the next start should be on the same date but the hour of the last start + delta
    # e.g. if the hour of the last start was 11, delta will be 1, and the next start will be at 12
    # on the other hand, if the hour of the last start was 15, the delta is 2.
    # So the next start will be 15 + 2 = 17
    return DateTime.combine(
        last_start.date(), Time(last_start.hour) + delta
    ).replace(tzinfo=UTC)
```

Well, in this case, we already had a previous dag run. 
We take the hour of that latest run and first check
whether the value is in the set that should be skipped.
If not, then the next run is simply an hour from now.

Otherwise, the next dag run hour falls into the range
12-17. So, let's calculate the delta. If the
last run was at 12. The delta will be:
17 - 12 = 5. 

However, I came up on an issue that arises if the last run 
hour is 17. In this case delta will be 0. 
Therefore, we need to fix the delta by setting it to 1.
Otherwise, the scheduler will enter an infinite loop.

## Limitations
There are a few limitations that you need to be aware of:
1. timetables are parsed during a dag run, so you should avoid slow and lengthy code and
connecting to databases or other external sources;
2. methods should always return the same result.

## Other examples
You can view the other examples from: 
1. airflow docs [here](https://airflow.apache.org/docs/apache-airflow/2.2.0/howto/timetable.html).
2. Astronomer [here](https://docs.astronomer.io/learn/scheduling-in-airflow).


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

The HttpTrigger in this repo returns the length of data
obtained. You can check how the value is accessed in the
deferrable operator:
```python
def execute_complete(self, context: Context, event: Optional[dict] = None) -> None:
    self.log.info(event)
    self.log.info("%s completed successfully.", self.task_id)
    self.log.info(event["data-length"])
    return event
```

Maybe the triggerer service should asynchronously write
large datasets to file or cloud storage? This seems the
most logical approach and seems to be hinted in the docs:
```text
Be especially careful when doing filesystem calls, as if the underlying filesystem is network-backed it may be blocking.
```

**IMPORTANT:** no state is persisted in a deferred operator.
The only way to pass state to a new instance of the operator
is via `kwargs` and `method_name`. Although, personally,
I fail to see how passing through `method_name` is useful.
You should also avoid having persistant state in the 
trigger. Everything that the trigger needs should be passed
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

Why number 3? Because deferrable operators should be
async from top to bottom. 

To demonstrate an API call, a wildfires-api service is
added with the `/api/get_with_delay` endpoint that accepts
a delay parameter telling the endpoint how long to wait
before returning the results.

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

Ideally, the module that you install will have its own 
repo on GitHub and you would install it from the repo 
while building the Dockerfile. In this case,
the module should be installed on both the scheduler
and triggerer. 

In my setup, it seems like the module needs to be installed
only on the triggerer. The dags have access to the module
because their located in a package structure that allows
them to find it. 

However, the BashOperator, and perhaps some others, won't
be able to find it because they are not executing
from the dags folder. 

**The logs** for the trigger instance (`HttpTrigger` in the 
example and its dependencies) **will show in the triggerer
service**, not the scheduler. Unless an exception is thrown
and the operator fails.

## PYTHONASYIONCIODEBUG
In the docker-compose.feat22.yml file we have:
```yaml
x-environment: &airflow_environment
...
  - PYTHONASYNCIODEBUG=1 # used to check whether the trigger was correctly implemented with all async calls
```
I'm not sure whether this variable is correctly set...

# References
1. https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html
2. https://registry.astronomer.io/providers/astronomer-providers/modules/httpsensorasync
3. https://airflow.apache.org/docs/apache-airflow/stable/modules_management.html
4. https://airflow.apache.org/docs/apache-airflow/2.2.0/howto/timetable.html
5. https://docs.astronomer.io/learn/scheduling-in-airflow

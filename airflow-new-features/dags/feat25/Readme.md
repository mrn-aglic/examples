# Airflow 2.5 new features

This subdirectory covers some features introduced in Airflow
2.5.

These include:
1. [x] Dynamic mapping over task groups
2. [ ] New @sensor decorator
3. [x] XCom updates for dynamically mapped tasks
4. [x] Updates to the datasets UI (demonstrated by default)


# Dynamic mapping over task groups
Dynamic mapping ver task groups allows us to have a 
dynamic number of task groups determined at runtime of
the DAG - based on the output of some previous operator.
It is similar to how dynamic task mapping works.

However, I have, at the time of writing, not found a way
to define and use dynamic task group mapping using the 
classical notation. That means, as far as I know,
currently you need to use a `@task_group` decorator
defined task group to use it. The classical `TaskGroup`
doesn't seem to implement the expand method and the code
documentation suggests creating mapped task groups by
calling `expand` or `expand_kwargs` on the decorated 
function [1].

There are two DAG examples for dynamic mapping over task
groups in this repo:
1. simple_task_group_mapping
2. task_group_mapping_example

## DAG simple_task_group_mapping

Most of this DAG is taken from an Astronomer webinar.

We define the DAG:
```python
with DAG(
    dag_id="simple_task_group_mapping",
    start_date=pendulum.now().subtract(hours=HOURS_AGO),
    schedule="0 * * * *",
    render_template_as_native_obj=True,
    on_success_callback=cleanup_xcom,
    description="This DAG demonstrates the use of task groups with dynamic task mapping using constant values",
    tags=["airflow2.5", "task_group_mapping"],
):
```


The DAG definition is something you'll probably see, 
except the `on_success_calback` and 
`render_template_as_native_object`. The 
`render_template_as_native_object` parameter tells Airflow
to parse XCom values from string to native Python 
objects. I have implemented a `on_success_callback` 
function that cleans XCom values for each dag run. 
I'm not going to explain the function here, you can 
check it out in the code.

The DAG starts with an operator that will produce the
elements to map over:
```python
def _obtain_elements():
    return [
        {"number": 1},
        {"number": 2},
        {"number": 3},
        {"number": 4},
        {"number": 5},
    ]
    
res = PythonOperator(task_id="obtain_elements", python_callable=_obtain_elements)
```

Our operator returns a list of dictionaries with a single
key-value pair. 

We define the task group using the decorator `@task_group`:
```python
@task_group(group_id="process_values")
def process(element):
    @task
    def extract_number(el):
        return el["number"]

    @task
    def add_42(num):
        return num + 42

    add_42(extract_number(element))
```

Each mapped instance of the task group will receive
one element from `res`.
There are two tasks in the task group. The first one
is used to extract the number from the obtained element
while the other one is used to add some value to the
number. The dependency between the task is defined
by passing the result of one function to the other:
```python
add_42(extract_number(element))
```

The final operator is used to sum the values from the
`add_42` operators. Here is the definition:
```python
def _sum_values(elements):
    print(f"elements: {elements}")
    return sum(elements)

sum_values = PythonOperator(
    task_id="sum_values",
    python_callable=_sum_values,
    op_kwargs={"elements": "{{ ti.xcom_pull(task_ids='process_values.add_42') }}"},
)
```

The important part here is how we obtained the elements
from the mapped operators using XCom:
```python
ti.xcom_pull(task_ids='process_values.add_42')
```
we're using `task_ids` here. And our `_sum_values` 
function will obtain the list of results.

Connecting the operators together: 
```python
res = PythonOperator(task_id="obtain_elements", python_callable=_obtain_elements)

process_node = process.expand(element=res.output)
process_node >> sum_values
```

The important method here is `process.expand` where
process is the task group decorated function and expand
tells the task group to be mapped over the output
of `res` task. At the time of writing, the expand
method doesn't exist on the `TaskGroup` instance.

## DAG task_group_mapping_example

A more concrete example of task group mapping. 
This DAG does the following:
- obtains the number of elements in some csv file located
in the `wildfires_api` service using the API endpoint;
- prepares a list of elements that will tell each
mapped task group from which row to start obtaining data.
I call this preparing batches, although actually we're
just preparing the start points for the batches;
- the task group contains two operators:
  - one for obtaining the data from the API and storing
  to S3 (Minio)
  - the second for downloading the data from S3 and 
  counting the number of fires per year. It then returns
  this result as JSON
  - the task group is mapped so that each mapped instance
  operates on a single batch
- counts the total number of fires per year and the
number of fires overall. The result is logged.

#### Obtaining the number of elements 

```python
get_rows_count = BashOperator(
    task_id="get_rows_count",
    bash_command=f"curl http://wildfires-api:8000/api/{COUNT_ENDPOINT}",
    do_xcom_push=True,
)
```

#### Prepare batch starting points
```python
def _create_batches(count):
    count = int(count)

    num_batches = math.ceil(count / BATCH_SIZE)
    return [{"start": i * BATCH_SIZE} for i in range(num_batches)]

create_batches = PythonOperator(
    task_id="create_batches",
    python_callable=_create_batches,
    op_kwargs={"count": "{{ ti.xcom_pull(task_ids='get_rows_count') }}"},
)
```

#### Task group definition
```python
@task_group(group_id="batch_processing")
def processing_group(my_batch):
    conn_id = "wildfires_api"
    s3_conn_id = "locals3"
    endpoint = GET_ENDPOINT

    @task
    def transfer_to_s3(single_batch):
        _transfer_to_s3(
            conn_id=conn_id,
            s3_conn_id=s3_conn_id,
            endpoint=endpoint,
            batch=single_batch,
        )

    @task
    def count_fire_per_year():
        return _count_per_year(s3_conn_id=s3_conn_id)

    transfer_to_s3(my_batch) >> count_fire_per_year()
```

The implementation of the functions `_transfer_to_s3`
and `_count_per_year` is omitted for brevity. 

#### Count the total number of fires
```python
final_count = PythonOperator(
    task_id="final_count",
    python_callable=_final_count,
    op_kwargs={
        "elements": "{{ ti.xcom_pull(task_ids='batch_processing.count_fire_per_year') }}"
    },
)
```

The function `_final_count` is omitted for brevity.

### Defining the dependencie

The dependencies are defined as follows:
```python
get_rows_count >> create_batches
pg = processing_group.expand(my_batch=create_batches.output)

pg >> final_count
```

# XCom updates for dynamically mapped tasks

Airflow from version 2.5.0 allows tasks to pull
XCom values over specific map indexes [2].

We can use XCom to pull only the results of specific
mapped tasks by index. The DAG `pull_specific_indexes`
demonstrates this. 

We start with an operator get some elements:
```python
def _obtain_elements():
    return [
        {"number": 1},
        {"number": 2},
        {"number": 3},
        {"number": 4},
        {"number": 5},
    ]

res = PythonOperator(task_id="obtain_elements", python_callable=_obtain_elements)
```

The operator `to_map` is the one that is being mapped
over:
```python
def _mapped_tasks(number, **context):
    task_instance = context["ti"]
    map_index = task_instance.map_index
    print(f"{task_instance.task_id}, index {map_index} got element: {number}")
    return number


to_map = PythonOperator.partial(
    task_id="mapped",
    python_callable=_mapped_tasks,
).expand(op_kwargs=res.output)
```

I demonstrate the pulling of specific mapped values
using the `xcom_pull` method both in a function and
using ninja templating:
```python
def _puller(**context):
    task_instance = context["ti"]
    elements = task_instance.xcom_pull(
        task_ids='mapped',
        map_indexes=[1, 4],
    )
    print(f"Got elements: {elements}")


puller = PythonOperator(
    task_id="puller",
    python_callable=_puller,
)

def _puller_ninja(elements):
    print(f"Got elements: {elements}")

puller_ninja = PythonOperator(
    task_id="puller_ninja",
    python_callable=_puller_ninja,
    op_kwargs={"elements": "{{ ti.xcom_pull(task_ids='mapped', map_indexes=[2, 3]) }}"}
)
```

The `map_indexes` parameter tells xcom which mapped values
we want to pull. 

Finally, we declare the rest of the dependencies:
```python
to_map >> [puller, puller_ninja]
```

The `res >> to_map` dependency is actually defined using
the expand function. 

Note that using `map_indexes` is intended for pulling
the values from mapped tasks. If you try to use it to
pull the values of a non mapped task, e.g. like `res`
in this example, you'll get an empty list for all indexes
except 0. For the 0 index you'll get the first element
of the list as an array.


# References
1. https://github.com/apache/airflow/blob/main/airflow/utils/task_group.py
2. https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html#new-features

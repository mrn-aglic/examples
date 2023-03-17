#!/bins/sh


DAG_ID=task_group_mapping_example
TASK_ID=task_id=batch_processing.count_fire_per_year
MAP_INDEX=0

SCHEDULER_CONTAINER_NAME=airflow-feat-scheduler

echo "$SCHEDULER_CONTAINER_NAME"

DIR="logs/dag_id=$DAG_ID/"

LAST_RUN_DIR=$(docker exec "$SCHEDULER_CONTAINER_NAME" ls -Artls $DIR | tail -1)

LAST_RUN_DIR=$(echo "$LAST_RUN_DIR" | rev | cut -d" " -f1 | rev)

CURR_DIR="$DIR/$LAST_RUN_DIR"

TARGET_DIR="$CURR_DIR/$TASK_ID"

if [ $MAP_INDEX -ge 0 ]
then
  TARGET_DIR="$TARGET_DIR/map_index=$MAP_INDEX"
fi

docker exec "$SCHEDULER_CONTAINER_NAME" ls -Artls "$TARGET_DIR"

LAST_RUN_ATTEMPT=$(docker exec "$SCHEDULER_CONTAINER_NAME" ls -Artls "$TARGET_DIR" | tail -1)
LAST_RUN_ATTEMPT=$(echo "$LAST_RUN_ATTEMPT" | rev | cut -d" " -f1 | rev)

TARGET_FILE="$TARGET_DIR/$LAST_RUN_ATTEMPT"

docker exec "$SCHEDULER_CONTAINER_NAME" cat "$TARGET_FILE"

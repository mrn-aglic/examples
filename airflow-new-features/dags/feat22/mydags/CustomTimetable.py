from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DataInterval, Timetable
from pendulum import DateTime, Time


class CustomTimetable(Timetable):
    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        dont_run = {i for i in range(8, 17)}
        hour = run_after.hour
        if hour in dont_run:
            interval = (
                min(dont_run)
                if hour >= min(dont_run) and hour < max(dont_run)
                else hour
            )
            delta = timedelta(hours=interval)
        else:
            delta = timedelta(hours=1)

        start = DateTime.combine((run_after - delta), Time.min)
        return DataInterval(start=start, end=())


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [CustomTimetable]

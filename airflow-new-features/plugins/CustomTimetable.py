import logging
import typing
from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from isodate import UTC
from pendulum import Date, DateTime, Time


class CustomTimetable(Timetable):
    dont_run = set(range(12, 18))  # We don't want to schedule runs between 12 and 17

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        # run_after is the time when the DAG was externally triggered
        hour = run_after.hour

        logging.info("Inferring manual run")

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

    def _get_next_start_prev_run_exists(
        self, last_automated_data_interval: DataInterval | None
    ) -> DateTime:
        # this method is executed if this is not the first scheduled DAG run
        last_start = last_automated_data_interval.start

        # get the hour of the last run
        last_start_hour = last_start.hour

        hour_delta = None

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

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: typing.Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:

        logging.info("next dagrun info executing")

        if last_automated_data_interval is not None:
            next_start = self._get_next_start_prev_run_exists(
                last_automated_data_interval
            )

        else:
            next_start = self._get_next_start_with_no_prev_run(restriction)

        logging.info("Calculated next_start: %s", next_start)
        # latest refers to the latest dag run that the dag can be scheduled, calculated from
        # end_date arguments
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.

        # Once we obtained the start of the next_run, setup the interval
        return DagRunInfo.interval(
            start=next_start, end=(next_start + timedelta(hours=1))
        )


class CustomTimetablePlugin(AirflowPlugin):
    name = "custom_timetable"
    timetables = [CustomTimetable]

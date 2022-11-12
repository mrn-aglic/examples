from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from isodate import UTC
from pendulum import Date, DateTime, Time


class CustomTimetable(Timetable):
    dont_run = set(range(12, 17))

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
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

        print("start:")
        print(start)
        print("DataIntervaL:")
        print(DataInterval(start=start, end=(start + timedelta(hours=1))))

        # Move the end by 1 hour
        return DataInterval(start=start, end=(start + timedelta(hours=1)))

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
            # we skip the hour to be at 17
            # e.g. last_start_hour = 15. 17 - 15 = 2 -> the delta
            hour_delta = max(self.dont_run) - last_start_hour
            delta = timedelta(hours=hour_delta)

        # the next start should be on the same date but the hour of the last start + delta
        # e.g. if the hour of the last start was 11, delta will be 1, and the next start will be at 12
        # on the other hand, if the hour of the last start was 15, the delta is 2.
        # So the next start will be 15 + 2 = 17
        return DateTime.combine(last_start.date(), Time(last_start.hour) + delta)

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

        elif next_start.time().minute != 0:
            # If catchup is enabled and the minute is not 0. That is, the hour is, e.g. 18:01
            # Then skip to the next hour
            next_hour = Time(next_start.hour + 1)

            # combine the next start date (today) and the next hour
            next_start = DateTime.combine(next_start.date(), next_hour).replace(
                tzinfo=UTC
            )

        # check whether the next run hour is in the don't run range, i.e. 12-17
        is_hour_in_dont_run = next_start.hour in self.dont_run

        if (
            is_hour_in_dont_run
        ):  # If next start is in the hour not to run, go to next run hour.
            # e.g. if the hour is 12:00
            # 17 - 12 = 5
            next_run_hour = max(self.dont_run) - next_start.hour

            # the timedelta will 5 hours in the future
            delta = timedelta(hours=next_run_hour)

            next_start = next_start + delta

        # return the next start date
        return next_start

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction
    ) -> DagRunInfo | None:

        if (
            last_automated_data_interval is not None
        ):  # There was a previous run on the regular schedule.
            # last_start = last_automated_data_interval.start
            # last_start_hour = last_start.hour
            #
            # if last_start_hour not in dont_run:
            #     delta = timedelta(hours=1)
            # else:
            #     hour_delta = max(dont_run) - last_start_hour
            #     delta = timedelta(hours=hour_delta)
            #
            # next_start = (last_start + delta).replace(tzinfo=UTC)
            next_start = self._get_next_start_prev_run_exists(
                last_automated_data_interval
            )

        else:  # This is the first ever run on the regular schedule.
            # next_start = restriction.earliest
            # if next_start is None:  # No start_date. Don't schedule.
            #     return None
            #
            # current_hour = Time(DateTime.now().hour)
            #
            # if not restriction.catchup:
            #     # If the DAG has catchup=False, today is the earliest to consider.
            #     next_start = max(next_start, DateTime.combine(Date.today(), current_hour).replace(tzinfo=UTC))
            #
            # elif next_start.time().minute != 0:
            #     # If earliest does fall on midnight, skip to the next day.
            #     # next_hour = current_hour + timedelta(hours=1)
            #     next_hour = Time(next_start.hour + 1)
            #
            #     next_start = DateTime.combine(next_start.date(), next_hour).replace(tzinfo=UTC)
            #
            # is_hour_in_dont_run = next_start.hour in dont_run
            #
            # if is_hour_in_dont_run:  # If next start is in the hour not to run, go to next run hour.
            #     next_run_hour = max(dont_run) - next_start.hour
            #     delta = timedelta(hours=next_run_hour)
            #
            #     next_start = next_start + delta
            next_start = self._get_next_start_with_no_prev_run(restriction)

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

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.blocking import BlockingScheduler
import cron_flink

scheduler = BlockingScheduler()

TIME_ZONE = 'Asia/Shanghai'

job = scheduler.add_job(func=cron_flink.run,
                        kwargs=None,
                        trigger=CronTrigger.from_crontab(
                            "*/30 * * * *", timezone=TIME_ZONE),
                        id="stat",
                        name="stat",
                        replace_existing=True,
                        misfire_grace_time=1)

if __name__ == "__main__":
    scheduler.start()
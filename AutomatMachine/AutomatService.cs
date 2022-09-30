using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace AutomatMachine
{
    public static class AutomatService
    {
        public static void Start()
        {
            var timer = new System.Timers.Timer(1000);

            timer.Elapsed += timer_Elapsed;
            timer.Start();
        }

        static object LockObject = new object();

        static void timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //(sender as System.Timers.Timer).Interval = AutomatJob.Jobs.Count > 0 ? AutomatJob.Jobs.Min(j => j.Interval) < 1000 ? AutomatJob.Jobs.Min(j => j.Interval) : 1000 : 1000;

            (sender as System.Timers.Timer).Interval = 1000;

            foreach (var job in AutomatJob.Jobs)
            {
                DateTime dateTimeNow = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second, 0);

                if (job.NextWorkDate.Value < dateTimeNow && job.JobState != JobState.Running && job.JobState != JobState.Tasking)
                    job.NextWorkDate = DateTime.Now;

                var year = job.NextWorkDate.Value.Year;
                var month = job.NextWorkDate.Value.Month;
                var day = job.NextWorkDate.Value.Day;
                var hour = job.NextWorkDate.Value.Hour;
                var minute = job.NextWorkDate.Value.Minute;
                var second = job.NextWorkDate.Value.Second;


                var isEqual = year == dateTimeNow.Year && month == dateTimeNow.Month && day == dateTimeNow.Day && hour == dateTimeNow.Hour && minute == dateTimeNow.Minute && second == dateTimeNow.Second;

                if (isEqual && job.JobState != JobState.Running && job.JobState != JobState.Tasking)
                {
                    lock (LockObject)
                    {
                        if (isEqual && job.JobState != JobState.Running && job.JobState != JobState.Tasking)
                        {
                            job.JobState = JobState.Tasking;

                            Task.Run(() => job.RunAsync());
                        }
                    }
                }
            }
        }
    }
}

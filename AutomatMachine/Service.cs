using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace AutomatMachine
{
    public static class Service
    {
        public static void Start()
        {
            var timer = new Timer(1000);

            timer.Elapsed += timer_Elapsed;
            timer.Start();
        }

        static void timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //(sender as Timer).Interval = Job.Jobs.Count > 0 ? Job.Jobs.Min(j => j.Interval) : 1000;
            //(sender as System.Timers.Timer).Interval = AutomatJob.Jobs.Count > 0 ? AutomatJob.Jobs.Min(j => j.Interval) < 1000 ? AutomatJob.Jobs.Min(j => j.Interval) : 1000 : 1000;
            
            foreach (var job in Job.Jobs)
            {
                if (job.NextWorkDate.Value < DateTime.Now)
                    job.NextWorkDate = DateTime.Now;


                var hour = job.NextWorkDate.Value.Hour;
                var minute = job.NextWorkDate.Value.Minute;
                var second = job.NextWorkDate.Value.Second;

                var isEqual = hour == DateTime.Now.Hour && minute == DateTime.Now.Minute && second == DateTime.Now.Second;

                if (isEqual)
                    Task.Run(() => job.RunAsync());
            }
        }
    }
}

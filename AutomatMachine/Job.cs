using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AutomatMachine
{
    public enum JobState
    {
        Failed,
        Running,
        Stopped,
        Successful,
        WaitingForChildrenToComplete,
        WaitingToRun
    }

    public class JobLog
    {
        public Job Job { get; set; }

        public string ClassName { get; set; }

        public string FunctionName { get; set; }

        public string Description { get; set; }

        public JobLog(string description)
        {
            var methodBase = new StackTrace().GetFrame(2).GetMethod();

            ClassName = methodBase.DeclaringType.Name;

            FunctionName = methodBase.Name;

            Description = description;
        }
    }

    public class Job
    {
        public static List<Job> Jobs = new List<Job>();

        public Guid Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public Automat Automat { get; set; }

        public Action Action { get; set; }

        public Job ContinueWith { get; set; }

        public double Interval { get; set; }

        public DateTime? LastWorkDate { get; set; }
        public DateTime? NextWorkDate { get; set; }

        public bool IsContinuous { get; set; }
        public JobState JobState { get; set; }
        public int NumberOfWorking { get; set; }

        public List<JobLog> JobLogs = new List<JobLog>();

        #region Constructor

        public Job(string name, int interval = 1000, Guid? id = null, bool isContinuous = false)
        {
            Name = name;

            SetInterval(miliseconds: interval);

            Id = id ?? Guid.NewGuid();

            Jobs.Add(this);

            SetContinuous(isContinuous);

            JobState = AutomatMachine.JobState.WaitingToRun;
        }

        #endregion

        #region Functions

        public Job AddLog(JobLog jobLog)
        {
            JobLogs.Add(jobLog);

            jobLog.Job = this;

            return this;
        }

        public Job SetInterval(int days = 0, int hours = 0, int minutes = 0, int seconds = 0, int miliseconds = 0)
        {
            TimeSpan ts = new TimeSpan(days, hours, minutes, seconds, miliseconds);

            Interval = ts.TotalMilliseconds;

            NextWorkDate = DateTime.Now.AddMilliseconds(Interval);

            return this;
        }

        public Job SetAction(Action action)
        {
            Action = action;

            return this;
        }

        public Job Invoke()
        {
            if(JobState==AutomatMachine.JobState.Successful || JobState==AutomatMachine.JobState.Failed)
            {
                if (!IsContinuous)
                    return this;
            }

            if (JobState == AutomatMachine.JobState.Running)
                return this;

            if (Action != null)
            {
                try
                {
                    JobState = global::AutomatMachine.JobState.Running;

                    Action.Invoke();

                    LastWorkDate = DateTime.Now;

                    NextWorkDate = DateTime.Now.AddMilliseconds(Interval);

                    NumberOfWorking++;

                    JobState = global::AutomatMachine.JobState.Successful;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);

                    JobState = global::AutomatMachine.JobState.Failed;
                }
            }

            return this;
        }


        public virtual Job Run()
        {
            Invoke();

            if (ContinueWith != null)
                Task.Run(()=>ContinueWith.RunAsync());

            return this;
        }

        public virtual async Task<Job> RunAsync()
        {
            var task = new Task<Job>(new Func<Job>(Invoke));

            task.Start();

            if (ContinueWith != null)
                await task.ContinueWith(t => ContinueWith.RunAsync());


            //if (JobState==AutomatMachine.JobState.Successful && !IsContinuous)
            //{
            //    return this;
            //}

            //if (Action != null)
            //{
            //    var task = new Task(Action);

            //    LastWorkDate = DateTime.Now;

            //    NextWorkDate = DateTime.Now.AddMilliseconds(Interval);

            //    task.Start();

            //    if (ContinueWith != null)
            //        await task.ContinueWith(t => ContinueWith.RunAsync());

            //    await task;

            //    NumberOfWorking++;
            //}

            await task;

            return this;
        }
 

        public Job Stop()
        {
            JobState = global::AutomatMachine.JobState.Stopped;

            return this;
        }

        public Job SetContinuous(bool enable)
        {
            IsContinuous = enable;

            return this;
        }

        #endregion
    }
}

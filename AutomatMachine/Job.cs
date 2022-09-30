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
        WaitingToRun,
        Tasking
    }

    public class JobLog
    {
        public AutomatJob Job { get; set; }

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

    public class AutomatJob
    {
        public static List<AutomatJob> Jobs = new List<AutomatJob>();

        public Guid Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public Automat Automat { get; set; }

        public Action<AutomatJob> Action { get; set; }

        public AutomatJob ContinueWith { get; set; }

        public double Interval { get; set; }

        public DateTime? LastWorkDate { get; set; }
        public DateTime? NextWorkDate { get; set; }

        public bool IsContinuous { get; set; }
        public JobState JobState { get; set; }
        public int NumberOfWorking { get; set; }

        private bool OnlyWorkOnDeployment { get; set; }

        public List<JobLog> JobLogs = new List<JobLog>();

        #region Constructor

        public AutomatJob(string name, int interval = 1000, Guid? id = null, bool isContinuous = false)
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

        public AutomatJob AddLog(JobLog jobLog)
        {
            JobLogs.Add(jobLog);

            jobLog.Job = this;

            return this;
        }

        public AutomatJob SetInterval(int days = 0, int hours = 0, int minutes = 0, int seconds = 0, int miliseconds = 0, TimeSpan? initTime = null)
        {
            TimeSpan ts = new TimeSpan(days, hours, minutes, seconds, miliseconds);

            Interval = ts.TotalMilliseconds;

            var dateTimeNow = DateTime.Now;
            var dateTimeWithInterval = dateTimeNow.AddMilliseconds(Interval);

            if (initTime != null)
            {
                //TimeSpan durationToCompleteDay = (dateTimeNow.Date + new TimeSpan(23, 59, 0)).TimeOfDay.Subtract(dateTimeNow.AddMilliseconds(Interval).TimeOfDay);
                //TimeSpan duration = new TimeSpan(24, 0, 0).Subtract(dateTimeNow.TimeOfDay);

                TimeSpan durationToCompleteDay = (dateTimeNow.Date + new TimeSpan(23, 59, 0)).Subtract(dateTimeWithInterval);

                //if (dateTimeNow.Hour > initTime.Value.Hours)
                //{

                //}

                if (dateTimeWithInterval < dateTimeNow.AddDays(1).Date && durationToCompleteDay.TotalMilliseconds > 0)
                {
                    if (initTime < dateTimeNow.TimeOfDay)
                        dateTimeNow = dateTimeNow.AddDays(1);
                }
                else
                {
                    if (initTime < dateTimeNow.TimeOfDay)
                        dateTimeNow = dateTimeNow.AddMilliseconds(Interval);
                }

                NextWorkDate = dateTimeNow.Date + initTime.Value;
            }
            else
                NextWorkDate = dateTimeWithInterval;

            return this;
        }

        public AutomatJob SetAction(Action<AutomatJob> action)
        {
            Action = action;

            return this;
        }

        private static object AutomatJobActionLockObject = new object();

        public AutomatJob Invoke()
        {
            lock (AutomatJobActionLockObject)
            {
                if (JobState == AutomatMachine.JobState.Running)
                    return this;

                if (JobState == AutomatMachine.JobState.Successful || JobState == AutomatMachine.JobState.Failed)
                {
                    if (!IsContinuous)
                        return this;
                }

                if (Action != null)
                {
                    try
                    {
                        JobState = JobState.Running;

                        if (OnlyWorkOnDeployment)
                        {
                            if (!new string[] { "KARAR", "TASLAN", "REA", "egecgin" }.Contains(Environment.UserName))
                            {
                                Action.Invoke(this);
                            }
                        }
                        else
                        {
                            Action.Invoke(this);
                        }

                        LastWorkDate = DateTime.Now;

                        NextWorkDate = DateTime.Now.AddMilliseconds(Interval);

                        NumberOfWorking++;

                        JobState = JobState.Successful;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);

                        JobState = JobState.Failed;
                    }
                }

                return this;
            }
        }


        public virtual AutomatJob Run()
        {
            Invoke();

            if (ContinueWith != null)
                Task.Run(() => ContinueWith.RunAsync());

            return this;
        }

        public virtual async Task<AutomatJob> RunAsync()
        {
            JobState = JobState.Tasking;

            var task = new Task<AutomatJob>(new Func<AutomatJob>(Invoke));

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


        public AutomatJob Stop()
        {
            JobState = JobState.Stopped;

            return this;
        }

        public AutomatJob SetContinuous(bool enable)
        {
            IsContinuous = enable;

            return this;
        }

        public AutomatJob OnlyWorkOnDeploymentMode()
        {
            OnlyWorkOnDeployment = true;

            return this;
        }

        #endregion
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AutomatMachine
{
    public class Automat
    {
        #region Properties

        public Guid Id { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        #endregion

        #region Collections

        //public List<Job> Jobs { get; set; }

        public List<Job> Jobs
        {
            get
            {
                return Job.Jobs;
            }
        }

        #endregion

        #region Constructor

        public Automat() { }

        public Automat(string name,string description)
        {
            Name = name;

            Id = Guid.NewGuid();

            Description = description;
        }

        public Automat(Guid id, string name, string description)
        {
            Name = name;

            Id = id;

            Description = description;
        }

        //private void Initialize()
        //{
        //    Jobs = new List<Job>();
        //}

        #endregion

        #region Functions

        public Automat AddJob(Job job)
        {
            job.Automat = this;

            return this;
        }

        #endregion
    }
}

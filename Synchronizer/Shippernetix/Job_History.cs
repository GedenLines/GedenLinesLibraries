using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Job_History : ShippernetixDynamicObject
    {
        public Job_History(string callSign, string l1, string l2, string l3, string l4, string jobCode, string jobNumber)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("Je_CallSign", callSign);


            if (!string.IsNullOrEmpty(l1))
                parameters.Add("Je_L1", l1);


            if (!string.IsNullOrEmpty(l2))
                parameters.Add("Je_L2", l2);


            if (!string.IsNullOrEmpty(l3))
                parameters.Add("Je_L3", l3);


            if (!string.IsNullOrEmpty(l4))
                parameters.Add("Je_L4", l4);


            if (!string.IsNullOrEmpty(jobCode))
                parameters.Add("Je_JobCode", jobCode);


            if (!string.IsNullOrEmpty(jobNumber))
                parameters.Add("Je_JobNo", jobNumber);

            Table = new DynamicTable(tableName: "Job_History", parameters: parameters)
                        .SetUniqueColumns("Je_CallSign", "Je_L1", "Je_L2", "Je_L3", "Je_L4", "Je_JobCode", "Je_JobCode", "Je_JobNo")
                        .PrepareUpdateAndInsertQueries();
        }

    }

}

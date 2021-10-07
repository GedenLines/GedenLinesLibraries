using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Job_Definition : ShippernetixDynamicObject
    {
        public Job_Definition(string callSign, string l1, string l2, string l3, string l4, string jobCode)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("Jd_CallSign", callSign);

            if (!string.IsNullOrEmpty(l1))
                parameters.Add("Jd_L1", l1);

            if (!string.IsNullOrEmpty(l2))
                parameters.Add("Jd_L2", l2);

            if (!string.IsNullOrEmpty(l3))
                parameters.Add("Jd_L3", l3);

            if (!string.IsNullOrEmpty(l4))
                parameters.Add("Jd_L4", l4);

            if (!string.IsNullOrEmpty(jobCode))
                parameters.Add("Jd_JobCode", jobCode);

            Table = new DynamicTable(tableName: "Job_Definition",
                parameters: parameters).SetUniqueColumns("Jd_CallSign", "Jd_L1", "Jd_L2", "Jd_L3", "Jd_L4", "Jd_JobCode")
                .PrepareUpdateAndInsertQueries();
        }
    }
}

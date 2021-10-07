using SqlManagement;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer
{
    public class ShippernetixDynamicObject
    {
        public DynamicTable Table { get; set; }

        public List<dynamic> PrepareDynamicRows()
        {
            var dynamicRows = new List<dynamic>();

            foreach (var row in Table.Rows)
            {
                dynamic jobRow = new ExpandoObject();

                foreach (var column in row.Columns)
                {
                   LoadExpandoObject(jobRow, column.Name, column.Value);
                }

                dynamicRows.Add(jobRow);
            }

            return dynamicRows;
        }

        public static void LoadExpandoObject(ExpandoObject eo, string key, object value)
        {
            var expandoDict = eo as IDictionary<string, object>;

            if (expandoDict.ContainsKey(key))
                expandoDict[key] = value;
            else
                expandoDict.Add(key, value);
        }
    }
}

using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer.Shippernetix
{
    public class Vessel_Master : ShippernetixDynamicObject
    {
        private static List<Vessel_Master> vessels { get; set; }
        public static List<Vessel_Master> Vessels
        {
            get
            {
                return vessels ?? (vessels = GetAllVessel());
            }
        }

        public string CallSign { get; set; }

        public string Name { get; set; }

        public string ConnectionString { get; set; }

        public Vessel_Master() { }

        public Vessel_Master(string callSign)
        {
            var parameters = new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(callSign))
                parameters.Add("CallSign", callSign);

            parameters.Add("Active", 1);

            Table = new DynamicTable(tableName: "Vessel_Master",
                parameters: parameters).SetUniqueColumns("CallSign")
                .PrepareUpdateAndInsertQueries();
        }

        public static List<Vessel_Master> GetAllVessel()
        {
            return new Vessel_Master(null).PrepareDynamicRows()
                                            .Select(r =>
                                            new Vessel_Master()
                                            {
                                                CallSign = r.CallSign,
                                                Name = r.Name,
                                                ConnectionString = CustomConnection.ConnectionStringPool.FirstOrDefault(s => s.Key == r.CallSign).Value
                                            }).ToList();
        }
    }
}

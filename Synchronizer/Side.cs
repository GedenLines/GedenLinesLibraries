using SqlManagement;
using Synchronizer.Shippernetix;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Synchronizer
{
    public class Side
    {
        public string Name { get; set; }

        public string ConnectionString { get; set; }

        public CustomConnection Connection { get; set; }

        public bool  PrepareForVesselSide { get; set; }

        public Side(string name,string connectionString, bool prepareForVesselSide)
        {
            Name = name;

            ConnectionString = connectionString;

            Connection = new MsSqlConnection(connectionString: connectionString);

            PrepareForVesselSide = prepareForVesselSide;
        }

        public Side Reconnect()
        {
            Connection = new MsSqlConnection(connectionString: ConnectionString);

            return this;
        }
        
    }
}

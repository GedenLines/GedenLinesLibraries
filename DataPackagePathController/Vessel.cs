using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlManagement;

namespace DataPackagePathController
{
    public class Vessel
    {
        public string Name { get; set; }

        public string CallSign { get; set; }

        public string Email { get; set; }


        public static List<Vessel> FetchAll()
        {
            var results = SqlManager.ExecuteQuery("select Name,CallSign,Email from Vessel_Master where Active=1", null, new MsSqlConnection(connectionString: "MsSqlConnectionString"));

            var list = results.Select(r => new Vessel()
            {
                Name = r["Name"].ToString(),
                CallSign = r["CallSign"].ToString(),
                Email = r["Email"].ToString()
            }).ToList();

            return list;
        }
    }
}

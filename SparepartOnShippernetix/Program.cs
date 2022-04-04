using FileManagement;
using FileManagement.FileType;
using SqlManagement;
using System;
using System.Collections.Generic;
using System.Text;

namespace SparepartOnShippernetix
{
    class Program
    {
        static void Main(string[] args)
        {
            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "MsSqlConnectionString",
                        "Server=BRIDGE; Database=GENEL; User id=sa;Password=;Application Name=Genel;Max Pool Size=50;MultipleActiveResultSets=true"
                    },
                };
            });


            var connection = new MsSqlConnection(connectionString: "MsSqlConnectionString");


            if (!SqlManager.CheckConnection(connection))
                throw new Exception("Db Connection Problem Occured");


            var path = CustomFile.CombinePaths(CustomFile.CurrentUserDesktopPath, "SparePart_1_31.csv");

            var file = new Dsv(path: path);

            var rows = file.Split();

            var parameters = new Dictionary<string, object>();

            var callSign = "9VLU";

            foreach (var row in rows)
            {
                var expectedColumnsLength = 10;

                var columns = row.Split(file.Separator, expectedColumnsLength, StringSplitOptions.RemoveEmptyEntries);

                if (columns.Length < expectedColumnsLength-1)
                    continue;

                var trimAction = new Func<string, string>((str) =>
                {
                    return str.Trim(',').Trim();
                });


                var strToReplace = @"""";

                var desc = columns[7].Replace("”", strToReplace);

                var sparePart = new Sparepart()
                {
                    S_Callsign = callSign,
                    S_Group = Convert.ToInt32(columns[0]),
                    S_Product = Convert.ToInt32(columns[1]),
                    S_Desc = desc,
                    S_MinQty = 0,
                    S_Critical =false,
                    S_Maker=null,
                    S_Unit= "PCE"
                };

                var b = Sparepart.Create(connection, sparePart);

                if (b)
                {
                    Console.WriteLine($"{sparePart.S_Callsign}-{sparePart.S_Group}-{sparePart.S_Product}-{sparePart.S_Desc}");
                }
                else
                {
                    Console.WriteLine("Error");
                }
            }
        }
    }
}

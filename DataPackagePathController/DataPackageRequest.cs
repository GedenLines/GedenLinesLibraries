using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataPackagePathController
{
    public class DataPackageRequest
    {
        public Guid Id { get; set; }

        public string CallSign { get; set; }

        public DateTime? LastWorkDate { get; set; }

        public int DataPackageNumber { get; set; }

        public string ProcessName { get; set; }

        public bool IsDone { get; set; }

        public string RequesterMail { get; set; }


        public static DateTime? ConvertToDateTime(object datetime)
        {
            if (datetime != null)
                return Convert.ToDateTime(datetime);

            return null;
        }

        public static List<DataPackageRequest> SelectRequests()
        {
            var list = new List<DataPackageRequest>();

            Console.WriteLine("Requests are being checked...");

            try
            {
                var sql = "select Id,CallSign,LastWorkDate,DataPackageNumber,ProcessName,IsDone,RequesterMail from DataPackageRequest where IsDone<>'1'";

                list = SqlManager.ExecuteQuery(sql, null, new MsSqlConnection("MsSqlConnectionString"))
                    .Select(r => new DataPackageRequest()
                    {
                        Id = Guid.Parse(r["Id"].ToString()),
                        CallSign = r["CallSign"].ToString(),
                        LastWorkDate = ConvertToDateTime(r["LastWorkDate"]),
                        DataPackageNumber = Convert.ToInt32(r["DataPackageNumber"]),
                        ProcessName = r["ProcessName"].ToString(),
                        IsDone = Convert.ToBoolean(r["IsDone"]),
                        RequesterMail = r["RequesterMail"].ToString()
                    })
                    .ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            return list;
        }

        public static bool CompletePackageRequest(Vessel vessel, Guid id, int dataPackageNumber)
        {
            int arc = 0;

            try
            {
                var sql = "update DataPackageRequest set IsDone=@IsDone,ModifiedDate=@ModifiedDate,LastWorkDate=@LastWorkDate,DataPackageNumber=@DataPackageNumber where Id=@Id";

                arc = SqlManager.ExecuteNonQuery(sql, new Dictionary<string, object>()
                {
                    {"IsDone",true },
                    {"ModifiedDate",DateTime.Now },
                    {"LastWorkDate",DateTime.Now },
                    {"Id",id },
                    {"DataPackageNumber", dataPackageNumber}
                },
                new MsSqlConnection(connectionString: "MsSqlConnectionString"));
            }
            catch (Exception ex)
            {
                Console.WriteLine("CompletePackageRequest error");
                Console.WriteLine(ex);
            }

            if (arc > 0)
            {
                Console.WriteLine($"The Request with id : '{id}' was completed successfully and a data package with number : {dataPackageNumber} was sent to {vessel.Name}({vessel.Email})");

                return true;
            }
            else
            {
                Console.WriteLine($"The Request with id : '{id}' was failed,please check the exception occured above");

                return false;
            }
        }

        public static bool CreatePackageRequest(string callSign, string processName, string requesterMail)
        {
            int arc = 0;

            try
            {
                var sql = "insert into DataPackageRequest([Id],[CallSign],[ProcessName],[IsDone],[RequesterMail],[AddedDate],[ModifiedDate]) " +
                    " values(@Id,@CallSign,@ProcessName,@IsDone,@RequesterMail,@AddedDate,@ModifiedDate)";

                arc = SqlManager.ExecuteNonQuery(sql, new Dictionary<string, object>()
                {
                    {"Id",Guid.NewGuid() },
                    {"CallSign", callSign},
                    {"ProcessName", processName},
                    {"IsDone", false},
                    {"RequesterMail", requesterMail},
                    {"AddedDate", DateTime.Now},
                    {"ModifiedDate", DateTime.Now}
                },
                new MsSqlConnection(connectionString: "MsSqlConnectionString"));
            }
            catch (Exception ex)
            {
                throw ex;
            }

            if (arc > 0)
                return true;
            else
                return false;
        }
    }
}

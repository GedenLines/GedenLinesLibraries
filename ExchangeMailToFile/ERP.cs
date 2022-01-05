using SqlManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExchangeMailToFile
{
    public class ERP
    {
        public static string OracleConnection { get; set; }

        public static bool InsertToT_RECEIVEDFILES(string fileName,string fileType,byte[] fileContent,string mailId,DateTime mailDate)
        {
            var clipper = new Func<string,string>((str) =>
            {
                return new string(str.TakeLast(20).ToArray());
            });


            var isCreated = false;

            SqlManagement.OracleConnection oc = new SqlManagement.OracleConnection(connectionString: "OracleConnectionString");

            if (SqlManager.CheckConnection(oc))
            {
                try
                {
                    //var maxFileNumber = SqlManager.ExecuteScalar("select max(FILEID) from T_RECEIVEDFILES") as int;

                    var affectedRowsCount = SqlManager.ExecuteNonQuery("insert into T_RECEIVEDFILES (FILEID,FILENAME,FILETYPE,FILECONTENT,MAILID,MAILDATE,CREDATE) values(SQ_RECFILE.NEXTVAL,:filename,:fileType,:fileContent,:mailId,:mailDate,:mailDate)",
                        new Dictionary<string, object>() 
                        {
                            { "filename",clipper(fileName)},
                            { "fileType",fileType},
                            { "fileContent",fileContent},
                            { "mailId",mailId},
                            { "mailDate",mailDate}
                        },
                        connection: oc);

                    if (affectedRowsCount > 0)
                    {
                        isCreated = true;

                        Console.WriteLine("The file ({0}) has been added to Oracle DB", fileName);
                    }
                    else
                    {
                        throw new Exception(string.Format("No records affected by ({0})'s inserting process to Oracle DB", fileName));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("An error occurred while adding the file({0}) into Oracle DB", fileName);

                    Console.WriteLine(ex.Message);

                    throw;
                }
            }

            return isCreated;
        }
    }
}

using Microsoft.Exchange.WebServices.Data;
using Microsoft.Extensions.Configuration;
using SqlManagement;
using System;
using System.Collections.Generic;
using AutomatMachine;
using FileManagement;
using System.Linq;
using System.IO;
using System.Net.Mail;
using MailManagement;
using System.Threading;

namespace DataPackagePathController
{
    class Program
    {
        public static string ReceivedDataFilePath { get; set; }

        public static string SendDataFilePath { get; set; }

        public static bool AllowDeleteMail { get; set; }

        public static int IntervalToReceiveDataAsMinute { get; set; } = 1;

        public static int IntervalToSendDataAsMinute { get; set; } = 1;

        public static int IntervalToCheckRequestsAsSeconds { get; set; }

        private static object RequestSenderLockObject { get; set; }

        static void Main(string[] args)
        {
            try
            {
                Initialize();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);

                Console.ReadLine();
            }

            //new MailManager(o => o.Code == "GedenErp")
            //                .Prepare(new Mail(from: new MailAddress("shippernetix@gedenlines.com"), to: null, subject: $"Shippernetix Data File", body: $"Shippernetix Data File.")
            //                            .AddTo(new MailAddress("karar@gedenlines.com"))
            //                            .AddCC(new MailAddress("shippernetix@gedenlines.com")))
            //                .Send((ex) =>
            //                {
            //                    Console.WriteLine(ex.Message);
            //                });


            Automat automat = new Automat("DataPackagePathController Automat", "Receiving or Sending Data Packages Using Exchange");

            var receivingDataJob = new AutomatJob("Receive Data")
                //.SetInterval(minutes:IntervalToReceiveDataAsMinute)
                .SetInterval(minutes: IntervalToReceiveDataAsMinute, initTime: new TimeSpan(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second + 5))
                .SetContinuous(true)
                .SetAction((j)=> 
                {
                    Console.WriteLine("");

                    if (!SqlManager.CheckConnection(new MsSqlConnection(connectionString: "MsSqlConnectionString")))
                        Console.WriteLine("There is a database connection problem,please check config file");
                    else
                    {
                        try
                        {
                            ReceivedData();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }

                        foreach (var job in AutomatJob.Jobs)
                        {
                            Console.WriteLine($"'{job.Name}' is gonna work at {job.NextWorkDate}(it worked last time {job.LastWorkDate})");
                        }
                    }
                });

            var sendingDataJob = new AutomatJob("Send Data")
                .SetInterval(minutes: IntervalToSendDataAsMinute)
                .SetContinuous(true)
                .SetAction((j)=>
                {
                    Console.WriteLine("");


                    if (!SqlManager.CheckConnection(new MsSqlConnection(connectionString: "MsSqlConnectionString")))
                        Console.WriteLine("There is a database connection problem,please check config file");
                    else
                    {
                        try
                        {
                            SendData();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    }

                    foreach (var job in AutomatJob.Jobs)
                    {
                        Console.WriteLine($"{job.Name} is gonna work at {job.NextWorkDate}(it worked last time {job.LastWorkDate})");
                    }
                });


            

            var requestObserver = new AutomatJob("Request Observer")
                .SetInterval(seconds: IntervalToCheckRequestsAsSeconds)
                .SetContinuous(true)
                .SetAction((j)=> 
                {
                    Console.WriteLine("");

                    if (!SqlManager.CheckConnection(new MsSqlConnection(connectionString: "MsSqlConnectionString")))
                        Console.WriteLine("There is a database connection problem,please check config file");
                    else
                    {
                        try
                        {
                            var requests = DataPackageRequest.SelectRequests();

                            foreach (var request in requests)
                            {
                                if (!request.IsDone)
                                {
                                    lock (RequestSenderLockObject)
                                    {
                                        if (!request.IsDone)
                                        {
                                            switch (request.ProcessName)
                                            {
                                                case "Send":

                                                    SendData(request);

                                                    break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    }
                });

            AutomatJob publishJob = new AutomatJob(name: "Publishing")
                       .SetContinuous(true)
                       .SetInterval(hours: 1, initTime: new TimeSpan(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second + 5))
                       .SetAction((j) =>
                       {
                           PublishingMail(j);
                       });

            automat.AddJob(receivingDataJob);
            automat.AddJob(sendingDataJob);
            automat.AddJob(publishJob);

            AutomatService.Start();

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        public static void ReceivedData()
        {
            Console.WriteLine("Receiving Data");

            var dictionary = ExchangeServerManager.Connect();

            if (dictionary.Count == 0)
                Console.WriteLine("No mail found to process");

            foreach (var item in dictionary)
            {
                var from = (item.Key as EmailMessage).From;

                var mailObject = new
                {
                    FromAddress = from.Address,
                    FromName = from.Name,
                    FileName = item.Value.Name,
                    FileData = item.Value.Content
                };

                var callSign = Vessel.FetchAll().Where(v => v.Email.ToLower() == from.Address.ToLower()).FirstOrDefault()?.CallSign;

                if (callSign == null)
                    continue;

                var file = new CustomFile(mailObject.FileName).SetData(mailObject.FileData);

                string cDiskPath = ReceivedDataFilePath ; //Path.GetPathRoot(Environment.SystemDirectory);
                 
                var path = CustomFile.CombinePaths(cDiskPath, callSign);

                CustomFile.CreateDirectoryIfNotExists(path);

                var pathToWrite = CustomFile.CombinePaths(path, mailObject.FileName);

                bool isWrittenToFile = false;

                try
                {
                    isWrittenToFile = file.WriteAllBytes(pathToWrite, file.Data);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    throw;
                }
                

                if (isWrittenToFile)
                {
                    Console.WriteLine($"The file with name{mailObject.FileName} has been created at {pathToWrite}");

                    if (isWrittenToFile && AllowDeleteMail)
                    {
                        if (ExchangeServerManager.DeleteMails(new List<ItemId>() { item.Key?.Id }))
                            Console.WriteLine("The mail with Id : {0} and Subject : {1} has been deleted", item.Key?.Id, item.Key?.Subject);
                        else
                            Console.WriteLine("An error occured while deleting the mail with Id : {0} and Subject : {1}", item.Key?.Id, item.Key?.Subject);
                    }
                }
                else
                {
                    Console.WriteLine($"There is an error while writing to file({mailObject.FileName}) at {pathToWrite}");
                }
            }
        }

        public static void SendData(DataPackageRequest request=null)
        {
            Console.WriteLine("Sending Data");

            string cDiskPath = SendDataFilePath;

            var directories = CustomFile.GetDirectoriesFromPath(cDiskPath);

            var callSignList = Vessel.FetchAll().Select(v => v.CallSign);

            foreach (var directory in directories)
            {
                if (!callSignList.Any(cs => cs == directory))
                    continue;

                var vessel = Vessel.FetchAll().FirstOrDefault(v => v.CallSign == directory);

                var newPathToRead = CustomFile.CombinePaths(cDiskPath, directory);

                if (request != null && vessel.CallSign != request.CallSign)
                    continue;

                if (!CustomFile.DirectoryExists(newPathToRead))
                    continue;

                var arc = SqlManager.ExecuteNonQuery(sql: $"Sp_SelectCache", parameters: new Dictionary<string, object>()
                {
                    {"FromOwnerCode",1 },
                    {"ToOwnerCode", vessel.CallSign}
                },
                    connection: new MsSqlConnection(connectionString: "MsSqlConnectionString"),
                isStoreProcedure: true);

                Console.WriteLine("Waiting 2 seconds...");
                Thread.Sleep(2000);

                var maxDataPackage = SqlManager.ExecuteScalar("select max(cast(Ds_PacketNo as int)) from Data_Send where Ds_ReceiveOwnerCode=@CallSign", new Dictionary<string, object>()
                    {
                        {"CallSign",vessel.CallSign }
                    }, new MsSqlConnection(connectionString: "MsSqlConnectionString"));

                Console.WriteLine($"Data package with number({maxDataPackage}) has been created");

                var attachmentsPathList = CustomFile.DirectoryGetFiles(newPathToRead);

                foreach (var attachmentPath in attachmentsPathList)
                {
                    Console.WriteLine($"Attachment Path is found : {attachmentPath}");

                    var attachmentFile = new CustomFile(path: attachmentPath, createNewIfNotExists: false);

                    Console.WriteLine($"Attachment File -> Path : {attachmentPath}, Name : {attachmentFile.NameWithExtension},AttachmentFile.Data.Length : {attachmentFile.Data?.Length},LengthAsMb : {attachmentFile.LengthAsMb}");

                    if (attachmentFile.Data == null || attachmentFile.Data?.Length == 0)
                        continue;

                    var isSentSuccessfully = false;

                    isSentSuccessfully = new MailManager(o => o.Code == "GedenErp")
                            .Prepare(new Mail(from: new MailAddress("shippernetix@gedenlines.com"), to: null, subject: $"Shippernetix Data File", body: $"Shippernetix Data File.")
                                        .AddTo(new MailAddress(vessel.Email))
                                        .AddCC(new MailAddress("shippernetix@gedenlines.com"))
                                        .AddAttachment(attachmentFile.Data, attachmentFile.Name, attachmentFile.Extension))
                            .Send((ex) =>
                            {
                                Console.WriteLine(ex.Message);
                            });

                    Console.WriteLine($"The mail was sent successfully : {isSentSuccessfully} , for the file named {attachmentFile.Name} in {newPathToRead}");

                    if (isSentSuccessfully)
                    {
                        var toPath = CustomFile.CombinePaths(newPathToRead, CustomFile.CombinePaths("Backup1"));

                        var destinationPath = CustomFile.CombinePaths(toPath, attachmentFile.NameWithExtension);

                        if (CustomFile.CreateDirectoryIfNotExists(toPath))
                        {
                            try
                            {
                                System.IO.File.Move(attachmentPath, destinationPath);

                                Console.WriteLine($"The file at {attachmentPath} has been moved to {destinationPath}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }
                        }

                        if (request!=null)
                        {
                            if(DataPackageRequest.CompletePackageRequest(vessel,request.Id, Convert.ToInt32(maxDataPackage)))
                            {
                                new MailManager(o => o.Code == "GedenErp")
                                        .Prepare(new Mail(from: new MailAddress("shippernetix@gedenlines.com"), to: null, subject: $"Request : Shippernetix Data Package('{vessel.Name}','{maxDataPackage}')",
                                        body: $"The data package(<b>{maxDataPackage}</b>) has been created at request of '<b>{request.RequesterMail}</b>' for '<b>{vessel.Name}</b>'(<b>{vessel.CallSign}</b>).")
                                                    .AddTo(new MailAddress(request.RequesterMail))
                                                    .AddCC(new MailAddress("bt@gedenlines.com")))
                                        .Send((ex) =>
                                        {
                                            Console.WriteLine(ex.Message);
                                        });
                            }
                        }
                    }
                }
            }
        }

        public static void Initialize()
        {
            var config = new ConfigurationBuilder()
                         .AddUserSecrets(typeof(ExchangeServerManager).Assembly)
                         .Build();

            var exchangeSettingsSection = config.GetSection("ExchangeSettings");

            var username = exchangeSettingsSection.GetSection("UserName").Value;
            var passsword = exchangeSettingsSection.GetSection("Password").Value;
            var domain = exchangeSettingsSection.GetSection("Domain").Value;
            var serviceUrl = exchangeSettingsSection.GetSection("ServiceUrl").Value;

            ExchangeServerManager.WebCredentials = new WebCredentials(username, passsword, domain);
            ExchangeServerManager.ServiceUrl = serviceUrl;

            ReceivedDataFilePath = config.GetSection("ReceivedDataFilePath").Value;
            SendDataFilePath = config.GetSection("SendDataFilePath").Value;
            AllowDeleteMail = config.GetSection("AllowDeleteMail").Value == "1";
            IntervalToReceiveDataAsMinute = Convert.ToInt32(config.GetSection("IntervalToReceiveDataAsMinute").Value);
            IntervalToSendDataAsMinute = Convert.ToInt32(config.GetSection("IntervalToSendDataAsMinute").Value);
            IntervalToCheckRequestsAsSeconds = Convert.ToInt32(config.GetSection("IntervalToCheckRequestsAsSeconds").Value);

            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "MsSqlConnectionString",
                        config.GetSection("MsSqlConnectionString").Value
                    }
                };
            });

            new MailManager("mail.gedenlines.com", 25, "shippernetix@gedenlines.com", "", "GedenErp", false);
        }

        private static void PublishingMail(AutomatJob job)
        {
            var html = $"DateTime.Now : {DateTime.Now},Environment Name : {Environment.UserName},job.Name : {job.Name},job.Interval : {job.Interval},job.LastWorkDate : {job.LastWorkDate},job.NextWorkDate : {job.NextWorkDate}";

            //var fromTimeSpanTo2DigitsString = new Func<TimeSpan, string>((ts) =>
            // {
            //     return $"{ts.Days} : {string.Format("{0:00}", ts.Hours)}:{string.Format("{0:00}", ts.Minutes)}:{string.Format("{0:00}", ts.Seconds)}";
            // });


            foreach (var automatJobGroup in AutomatJob.Jobs.Where(j => j.Parent == null).GroupBy(g => g.Interval))
            {
                html += "<br/>";
                html += $"<h2>{automatJobGroup.Key}</h2>";
                html += "<ul>";

                var counter = 0;
                var bgColor = "";
                var textColor = "black";

                foreach (var groupMember in automatJobGroup)
                {
                    counter++;

                    bgColor = counter % 2 != 0 ? "beige" : "white";
                    //textColor = counter % 2 == 0 ? "black" : "black";

                    html += $"<li style='background-color:{bgColor};color:{textColor}'>Name : <span style='color:green;'>{groupMember.Name}</span>,</br>Interval : {groupMember.Interval},</br>IsContinuous : {groupMember.IsContinuous},</br>State : {groupMember.JobState},</br>Last Work Date : {groupMember.LastWorkDate},</br>Next Work Date : {groupMember.NextWorkDate}</li>";
                }

                html += "</ul>";
            }

            var mailManager = new MailManager(o => o.Code == "GedenErp")
                  .Prepare(new Mail(from: new MailAddress("gedenerp@gedenlines.com"),
                                       to: new List<MailAddress>() { new MailAddress("karar@gedenlines.com") },
                                       subject: $"Mail On Publishing(Data Package Controller)",
                                       body: html));

            mailManager.Send();
        }
    }
}

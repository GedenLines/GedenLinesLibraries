using AutomatMachine;
using FileManagement;
using Microsoft.Exchange.WebServices.Data;
using Microsoft.Extensions.Configuration;
using SqlManagement;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ExchangeMailToFile
{
    //T_RECIEVEDFILES
    class Program
    {
        public static string PathToWriteInvoices { get; set; }
        static void Main(string[] args)
        {
            Initialize();

            Automat automat = new Automat("ExchangeServer Automat", "Fetching attachments from exchange server and writes them to oracle db and a file");

            var job = new Job("Job1")
                .SetInterval(seconds: 1)
                                    .SetContinuous(true)
                                    .SetAction(() =>
                                    {
                                        var dictionary = ExchangeServerManager.Connect();

                                        if (dictionary.Count == 0)
                                            Console.WriteLine("No mail found to process");

                                        foreach (Item item in dictionary.Keys)
                                        {
                                            var fileAttachment = dictionary[item];

                                            string fileName = fileAttachment.Name;
                                            string fileType = "pdf";
                                            byte[] fileContent = fileAttachment.Content;
                                            string mailId = item.Id.UniqueId;
                                            DateTime mailDate = item.DateTimeCreated;

                                            var path = CustomFile.CombinePaths(PathToWriteInvoices, DateTime.Today.ToString("yyyy_MM_dd"));

                                            CustomFile.CreateDirectoryIfNotExists(path);

                                            var isWrittenToFile = false;

                                            try
                                            {
                                                isWrittenToFile = new CustomFile(fileName, ".pdf").SetData(fileContent)
                                                                                                .WriteAllBytes(CustomFile.CombinePaths(path, fileName));
                                            }
                                            catch (Exception ex)
                                            {

                                                throw;
                                            }
                                            

                                            if (isWrittenToFile)
                                            {
                                                Console.WriteLine("The file({0}) has been created at({1})...", fileName, path);
                                            }
                                            else
                                            {
                                                var message = string.Format("An error occurred while creating the file ({0}) at ({1})...", fileName, path);

                                                Console.WriteLine(message);

                                                throw new Exception(message);
                                            }

                                            var isCreatedOnERP = ERP.InsertToT_RECEIVEDFILES(fileName,fileType,fileContent, mailId, mailDate);

                                            if (isCreatedOnERP && isWrittenToFile)
                                            {
                                                if (ExchangeServerManager.DeleteMails(new List<ItemId>() { item.Id }))
                                                    Console.WriteLine("The mail with Id : {0} and Subject : {1} has been deleted",item.Id,item.Subject);
                                                else
                                                    Console.WriteLine("An error occured while deleting the mail with Id : {0} and Subject : {1}", item.Id, item.Subject);

                                            }
                                        }
                                    });

            automat.AddJob(job);

            Service.Start();

            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        public static void Initialize()
        {
            var config = new ConfigurationBuilder()
                         .AddUserSecrets(typeof(ExchangeServerManager).Assembly)
                         .Build();

            var exchangeSettingsSection =  config.GetSection("ExchangeSettings");

            var username = exchangeSettingsSection.GetSection("UserName").Value;
            var passsword = exchangeSettingsSection.GetSection("Password").Value; 
            var domain = exchangeSettingsSection.GetSection("Domain").Value;
            var serviceUrl = exchangeSettingsSection.GetSection("ServiceUrl").Value;

            ExchangeServerManager.WebCredentials = new WebCredentials(username,passsword,domain);
            ExchangeServerManager.ServiceUrl = serviceUrl;

            ERP.OracleConnection = config.GetSection("OracleConnectionString").Value;

            PathToWriteInvoices = config.GetSection("PathToWriteInvoices").Value;

            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "OracleConnectionString",
                        ERP.OracleConnection
                    }
                };
            });

        }
    }
}

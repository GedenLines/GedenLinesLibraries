using AutomatMachine;
using MailManagement;
using Microsoft.Extensions.Configuration;
using SqlManagement;
using Synchronizer.Shippernetix;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Threading;

namespace Synchronizer
{
    public class Program
    {
        public static void SendMail(string subject,string message)
        {
            new MailManager(o => o.Code == "GedenErp")
                .Prepare(new Mail(new MailAddress("karar@gedenlines.com"), null, $"Synchronize Operation 101 / {subject}", $"{message}")
                            .AddTo(new MailAddress("karar@gedenlines.com")))
            .Send((ex) =>
            {
                Console.WriteLine(ex.Message);
            });
        }

        static void Main(string[] args)
        {
            Initialize();

            //for (int i = 0; i < Vessel_Master.Vessels.Count; i++)
            //{
            //    var vessel = Vessel_Master.Vessels[i];

            //    Automat automat = new Automat(vessel.CallSign,vessel.Name);

            //    automat.AddJob(new Job("Job_History",1000,null,true)
            //        .SetAction(()=> 
            //        {
            //            Console.WriteLine($"{vessel.Name} {vessel.CallSign}");
            //        }));

            //}

            Service.Start();




            //Shippernetix.Job_History.Fix("9HA4","4","3","3","1","A","2",new MsSqlConnection(connectionString: "9HA4"),new MsSqlConnection(connectionString: "MsSqlConnectionString"), false);
            //Shippernetix.Job_History.Fix("9HA4", "1", "2", "3", "2", "A", "2", new MsSqlConnection(connectionString: "9HA4"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), false);
            //Shippernetix.Job_History.Fix("9HA4", "8", "1", "1", "15", "A", "19", new MsSqlConnection(connectionString: "9HA4"), new MsSqlConnection(connectionString: "MsSqlConnectionString"), false);


            //Console.ReadLine();

            foreach (var vessel in Vessel_Master.Vessels)
            {
                if (vessel.CallSign != "9HM2")
                    continue;

                var source = new Side("Office", "MsSqlConnectionString",true);

                var isSourceConnected = SqlManager.CheckConnection(source.Connection);

                var target = new Side(vessel.Name, vessel.CallSign, false);

                var istargetConnected = SqlManager.CheckConnection(target.Connection);

                if (isSourceConnected && istargetConnected)
                {
                    // Structure.Sync(source, target,vessel);
                    //Defect.Sync(source,target,vessel);
                    Job_History.Sync(source,target,vessel,true);
                    //Job_Definition.Sync(source,target,vessel);
                }
                else
                    Console.WriteLine("Source({0}) is connected : {1},Target({2}) is Connected : {3}", source.Name, isSourceConnected, target.Name, istargetConnected);
            }
        }

        static void Initialize()
        {
            var config = new ConfigurationBuilder()
            .AddUserSecrets(typeof(Program).Assembly)
            .Build();

            CustomConnection.Start(() =>
            {
                return new Dictionary<string, string>()
                {
                    {
                        "MsSqlConnectionString",
                        config.GetConnectionString("Genel")
                    },
                    {
                        "9BLP",
                         config.GetConnectionString("9BLP")
                    },
                     {
                        "9PTY",
                         config.GetConnectionString("9PTY")
                    },
                    {
                        "9RYL",
                         config.GetConnectionString("9RYL")
                    },
                    {
                        "9HNK",
                         config.GetConnectionString("9HNK")
                    },
                    {
                        "9PRD",
                         config.GetConnectionString("9PRD")
                    },
                    {
                         "9PRK",
                         config.GetConnectionString("9PRK")
                    },
                    {
                        "9VLE",
                        config.GetConnectionString("9VLE")
                    },
                    {
                        "9HA4",
                        config.GetConnectionString("9HA4")
                    },
                    {
                        "9HM2",
                        config.GetConnectionString("9HM2")
                    }
                };
            });

            new MailManager("mail.gedenlines.com", 25, "karar@gedenlines.com", "koreaaria", "GedenErp", false);
        }
    }
}

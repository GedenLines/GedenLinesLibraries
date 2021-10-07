using Microsoft.Extensions.Configuration;
using SqlManagement;
using Synchronizer.Shippernetix;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Synchronizer
{
    class Program
    {
        static void Main(string[] args)
        {
            Initialize();

            foreach (var vessel in Vessel_Master.Vessels)
            {
                if (vessel.CallSign != "9PTY")
                    continue;

                var source = new Side("Office", "MsSqlConnectionString",false);

                var target = new Side(vessel.Name,vessel.CallSign,false);

                var isSourceConnected = SqlManager.CheckConnection(source.Connection);
                var istargetConnected = SqlManager.CheckConnection(target.Connection);

                if (isSourceConnected && istargetConnected)
                {
                    // Structure.Sync(source, target);
                    Defect.Sync(source,target,vessel);
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
                    }
                };
            });
        }
    }
}

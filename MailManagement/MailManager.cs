using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace MailManagement
{
    public class MailManager
    {
        public static List<SmtpClientPoolObject> SmtpClientPool = new List<SmtpClientPoolObject>();

        SmtpClient SmtpClient { get; set; }

        Mail Mail { get; set; }

        public MailManager(
                    string smtpClientHost,
                    int smtpClientPort,
                    string smtpClientUser,
                    string smtpClientPassword,
                    string poolCode = null,
                    bool enableSsl = true)
        {
            SmtpClient = new SmtpClient();

            if (string.IsNullOrWhiteSpace(smtpClientHost) || string.IsNullOrWhiteSpace(smtpClientUser) || string.IsNullOrWhiteSpace(smtpClientPassword))
                throw new Exception("One of these fields cannot be left blank(smtpClientHost,smtpClientUser,smtpClientPassword)");

            SmtpClient.Host = smtpClientHost;
            SmtpClient.Port = smtpClientPort;
            SmtpClient.UseDefaultCredentials = false;
            SmtpClient.Credentials = new NetworkCredential(smtpClientUser, smtpClientPassword);
            SmtpClient.DeliveryMethod = SmtpDeliveryMethod.Network;
            SmtpClient.EnableSsl = enableSsl;
            SmtpClient.SendCompleted += new SendCompletedEventHandler(SendCompletedCallback);

            AddSmtpClientToPool(new SmtpClientPoolObject(SmtpClient, poolCode));
        }

        /// <summary>
        /// Set your email account's security as less from the following link => https://myaccount.google.com/lesssecureapps
        /// </summary>
        /// <param name="smtpClientUser"></param>
        /// <param name="smtpClientPassword"></param>
        /// <param name="poolCode"></param>
        /// <param name="enableSsl"></param>
        /// <returns></returns>
        public static MailManager Gmail(string smtpClientUser,
                    string smtpClientPassword,
                    string poolCode = null,
                    bool enableSsl = true)
        {
            var smtpClientHost = "smtp.gmail.com";

            var smtpClientPort = 587;

            return new MailManager(smtpClientHost, smtpClientPort, smtpClientUser, smtpClientPassword, poolCode, enableSsl);
        }

        /// <summary>
        /// smtp.live.com	25, 587	TLS
        /// </summary>
        /// <param name="smtpClientUser"></param>
        /// <param name="smtpClientPassword"></param>
        /// <param name="poolCode"></param>
        /// <param name="enableSsl"></param>
        /// <returns></returns>
        public static MailManager Microsoft(string smtpClientUser,
                    string smtpClientPassword,
                    string poolCode = null,
                    bool enableSsl = true)
        {
            var smtpClientHost = "smtp.live.com";

            var smtpClientPort = 587;

            return new MailManager(smtpClientHost, smtpClientPort, smtpClientUser, smtpClientPassword, poolCode, enableSsl);
        }

        public MailManager(Func<SmtpClientPoolObject, bool> predicate)
        {
            ChangeSmtpClient(predicate);
        }

        public static void AddSmtpClientToPool(SmtpClientPoolObject poolObject)
        {
            if (!SmtpClientPool.Any(o => o.Id == poolObject.Id || o.Code == poolObject.Code))
            {
                SmtpClientPool.Add(poolObject);
            }
        }

        /// <summary>
        /// Use this function to pick SmtpClientObject you want from SmtpClientPool
        /// Usage : ChangeSmtpClient(scpo=>scpo.Code=="MyClientCode")
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public MailManager ChangeSmtpClient(Func<SmtpClientPoolObject, bool> predicate)
        {
            SmtpClient = SmtpClientPool.FirstOrDefault(predicate).Client;
            
            return this;
        }

        public MailManager Prepare(Mail mail)
        {
            Mail = mail;

            return this;
        }

        public bool Send(Action<Exception> logAction = null)
        {
            try
            {
                SmtpClient.Send(Mail.Message);

                return true;
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);

                return false;
            }
        }

        private void SendCompletedCallback(object sender, AsyncCompletedEventArgs e)
        {
            // Get the unique identifier for this asynchronous operation.
            String token = (string)e.UserState;

            if (e.Cancelled)
            {
                Console.WriteLine("[{0}] Send canceled.", token);
            }
            if (e.Error != null)
            {
                Console.WriteLine("[{0}] {1}", token, e.Error.ToString());
            }
            else
            {
                Console.WriteLine("Message sent.");
            }
        }
    }

    public class SmtpClientPoolObject
    {
        public Guid Id { get; set; }

        public string Code { get; set; }

        public SmtpClient Client { get; private set; }

        public SmtpClientPoolObject(SmtpClient client, string code = null)
        {
            Id = Guid.NewGuid();

            Client = client;

            Code = code;
        }
    }
}

using Microsoft.Exchange.WebServices.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Text;
using System.Threading.Tasks;

namespace ExchangeMailToFile
{
    public class ExchangeServerManager
    {
        public static WebCredentials WebCredentials { get; set; }

        public static string ServiceUrl { get; set; }

        public static Dictionary<Item, FileAttachment> Connect()
        {
            var dictionary = new Dictionary<Item, FileAttachment>();

            try
            {
                ExchangeService service = new ExchangeService();

                service.Credentials = WebCredentials;

                service.Url = new Uri(ServiceUrl);

                ServicePointManager.ServerCertificateValidationCallback = (RemoteCertificateValidationCallback)((_param1, _param2, _param3, _param4) => true);

                ItemView view = new ItemView(100);

                FindItemsResults<Item> findResults = service.FindItems(WellKnownFolderName.Inbox, view);
                    

                if (findResults != null && findResults.Items != null && findResults.Items.Count > 0)
                    foreach (Item item in findResults.Items)
                    {
                        EmailMessage message = EmailMessage.Bind(service, item.Id, new PropertySet(BasePropertySet.IdOnly, ItemSchema.Attachments, ItemSchema.HasAttachments));

                        var attachmentsCount = message.Attachments.Count;

                        foreach (var attachment in message.Attachments)
                        {
                            if (!attachment.Name.ToLower().EndsWith(".pdf"))
                                continue;

                            Console.WriteLine(item.Subject);

                            if (attachment is FileAttachment)
                            {
                                FileAttachment fileAttachment = attachment as FileAttachment;

                                fileAttachment.Load();

                                dictionary.Add(item,fileAttachment);

                                Console.WriteLine("\nAttachment name: " + fileAttachment.Name);
                            }
                        }
                    }
                else
                    Console.WriteLine("no items");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

                throw;
            }


            return dictionary;
        }

        public static bool DeleteMails(IEnumerable<ItemId> itemIdsToDelete)
        {
            var isDeleted = false;

            try
            {
                ExchangeService service = new ExchangeService();

                service.Credentials = WebCredentials;

                service.Url = new Uri(ServiceUrl);

                ServicePointManager.ServerCertificateValidationCallback = (RemoteCertificateValidationCallback)((_param1, _param2, _param3, _param4) => true);

                service.DeleteItems(itemIdsToDelete, DeleteMode.MoveToDeletedItems, null, null);

                isDeleted = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);

                throw;
            }

            return isDeleted;
        }

    }
}

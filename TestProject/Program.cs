using System;
using System.Net.Mail;
using MailManagement;

namespace TestProject
{
    class Program
    {
        static void Main(string[] args)
        {
            //"@SIB6U9pHeG!"
            new MailManager("mail.gedenlines.com",25, "gedenerp",null,null,false)
                .Prepare(new Mail(new MailAddress("gedenerp@gedenlines.com"), null, "Subject", "<b>Body</b>")
                                        .AddTo(new MailAddress("eakdas@gedenline.com")))
                        .Send((ex)=> 
                        {
                            Console.WriteLine(ex.Message);
                        });

            Console.ReadLine();
        }
    }
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;

namespace MailManagement
{
    public enum FileType
    {
        Html,
        Plain,
        RichText,
        Xml,
        Gif,
        Jpeg,
        Tiff,
        Octet,
        Pdf,
        Rtf,
        Soap,
        Zip
    }

    public class Mail
    {
        public Guid Id { get; set; }

        public MailMessage Message { get; set; }

        public Mail(MailAddress from, List<MailAddress> to, string subject, string body, bool isBodyHtml = true)
        {
            Id = Guid.NewGuid();

            Message = new MailMessage();

            Message.From = from;

            if (to != null)
                to.ForEach(t => Message.To.Add(t));

            Message.Subject = subject;

            Message.Body = body;

            Message.SubjectEncoding = Encoding.UTF8;
            Message.BodyEncoding = Encoding.UTF8;

            Message.IsBodyHtml = isBodyHtml;
        }

        public Mail AddTo(params MailAddress[] recipients)
        {
            foreach (var recipient in recipients)
            {
                Message.To.Add(recipient);
            }

            return this;
        }

        public Mail AddCC(params MailAddress[] ccs)
        {
            foreach (var cc in ccs)
            {
                Message.CC.Add(cc);
            }

            return this;
        }

        public static ContentType ConvertToContentType(FileType contentType)
        {
            var ct = new ContentType();

            switch (contentType)
            {
                case FileType.Html:
                    ct = new ContentType(MediaTypeNames.Text.Html);
                    break;

                case FileType.Plain:
                    ct = new ContentType(MediaTypeNames.Text.Plain);
                    break;

                case FileType.RichText:
                    ct = new ContentType(MediaTypeNames.Text.RichText);
                    break;

                case FileType.Xml:
                    ct = new ContentType(MediaTypeNames.Text.Xml);
                    break;

                case FileType.Gif:
                    ct = new ContentType(MediaTypeNames.Image.Gif);
                    break;

                case FileType.Jpeg:
                    ct = new ContentType(MediaTypeNames.Image.Jpeg);
                    break;

                case FileType.Tiff:
                    ct = new ContentType(MediaTypeNames.Image.Tiff);
                    break;

                case FileType.Octet:
                    ct = new ContentType(MediaTypeNames.Application.Octet);
                    break;

                case FileType.Pdf:
                    ct = new ContentType(MediaTypeNames.Application.Pdf);
                    break;

                case FileType.Rtf:
                    ct = new ContentType(MediaTypeNames.Application.Rtf);
                    break;

                case FileType.Soap:
                    ct = new ContentType(MediaTypeNames.Application.Soap);
                    break;

                case FileType.Zip:
                    ct = new ContentType(MediaTypeNames.Application.Zip);
                    break;
            }

            return ct;
        }

        public Mail AddAttachment(byte[] data, string fileName, FileType contentType,Action<Exception> logAction = null)
        {
            try
            {
                var ct = ConvertToContentType(contentType);

                var attachment = new Attachment(new MemoryStream(data), ct);
                attachment.ContentDisposition.FileName = string.Format("{0}.{1}", fileName, ct.MediaType.Split('/').Last());

                Message.Attachments.Add(attachment);

                return this;
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }

            return this;
        }
    }
}

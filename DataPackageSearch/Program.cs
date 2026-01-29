using SharpCompress.Archives.Rar;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace DataPackageSearch
{
    class Program
    {
        static void Main(string[] args)
        {
            string klasorYolu = @"C:\Users\rea\Desktop\yedek2";
            string aranacakKelime = @"delete Job_Definition";

            string[] rarDosyalari = Directory.GetFiles(klasorYolu, "*.rar");

            foreach (var rarDosyasi in rarDosyalari)
            {
                using (var archive = RarArchive.Open(rarDosyasi))
                {
                    foreach (var entry in archive.Entries.Where(entry => !entry.IsDirectory))
                    {
                        using (var entryStream = entry.OpenEntryStream())
                        using (var reader = new StreamReader(entryStream))
                        {
                            string icerik = reader.ReadToEnd();
                            if (icerik.Contains(aranacakKelime))
                            {
                                Console.WriteLine($"'{aranacakKelime}' bulundu: {rarDosyasi}");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

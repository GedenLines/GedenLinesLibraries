using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileManagement.FileType
{
    public interface IImage
    {
        string Alt { get; set; }
    }

    public class Image : CustomFile, IImage
    {
        #region Properties

        public string Alt { get; set; }

        public int Width
        {
            get
            {
                return drawingImage != null ? drawingImage.Width : 0;
            }
        }

        public int Height
        {
            get
            {
                return drawingImage != null ? drawingImage.Height : 0;
            }
        }

        public ImageFormat ImageFormat
        {
            get
            {
                return drawingImage != null ? drawingImage.RawFormat : null;
            }
        }

        System.Drawing.Image drawingImage = null;
        public System.Drawing.Image DrawingImage
        {
            get
            {
                int skip = 0;

                if (Data.Length > 4
                    && Data[0] == 1
                    && Data[1] == 0
                    && Data[2] == 0
                    && Data[3] == 1)
                    skip = 8;

                try
                {
                    var ms = new MemoryStream(Data, skip, Data.Length - skip);

                    return drawingImage ?? (drawingImage = System.Drawing.Image.FromStream(ms, true));
                }
                catch (Exception ex)
                {
                    if (LogAction != null)
                        LogAction(ex);

                    return null;
                }
            }
            set
            {
                drawingImage = value;
            }
        }

        public string Src
        {
            get
            {
                return string.Format("data:image/{0};base64,{1}", !string.IsNullOrWhiteSpace(Extension) ? Extension.Replace('.', ' ') : "png", Base64String);
            }
        }

        Size? thumbnailSize = null;
        public Size? ThumbnailSize
        {
            get
            {
                return thumbnailSize ?? (thumbnailSize = CalculateThumbnailSize(DrawingImage));
            }
            set
            {
                thumbnailSize = value;
            }
        }

        #endregion

        #region Constructor

        public Image(string path, string altText = "Alt")
            : base(path)
        {
            Alt = altText;
        }

        public Image(string fileName, string extension, string altText = "Alt")
            : base(fileName, extension)
        {
            Alt = altText;

            SetName(fileName);

            SetExtension(extension);
        }

        public Image() { }

        #endregion

        #region Functions

        public Image Resize(int width, int height)
        {
            var newImage =  new Image(Name, Extension);

            newImage.SetData(ImageToByteArray(Resize(DrawingImage, width, height)));

            return newImage;
        }

        public Image GetThumbnail()
        {
            Image newImage = new Image(Name, Extension);

            if (ThumbnailSize != null)
                newImage.SetData(ImageToByteArray(Resize(DrawingImage, ThumbnailSize.Value.Width, ThumbnailSize.Value.Height)));
            else
                newImage.SetData(ImageToByteArray(Resize(DrawingImage, 100, 100)));

            return newImage;
        }

        public static Bitmap Resize(System.Drawing.Image image, int width, int height,Action<Exception> logAction = null)
        {
            var destRect = new Rectangle(0, 0, width, height);
            var destImage = new Bitmap(width, height);

            try
            {
                destImage.SetResolution(image.HorizontalResolution, image.VerticalResolution);

                using (var graphics = Graphics.FromImage(destImage))
                {
                    graphics.CompositingMode = CompositingMode.SourceCopy;
                    graphics.CompositingQuality = CompositingQuality.HighQuality;
                    graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                    graphics.SmoothingMode = SmoothingMode.HighQuality;
                    graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                    using (var wrapMode = new ImageAttributes())
                    {
                        wrapMode.SetWrapMode(WrapMode.TileFlipXY);
                        graphics.DrawImage(image, destRect, 0, 0, image.Width, image.Height, GraphicsUnit.Pixel, wrapMode);
                    }
                }
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }

            return destImage;
        }


        public static Size? CalculateThumbnailSize(System.Drawing.Image original)
        {
            if (original == null)
                return null;

            // Maximum size of any dimension.
            const int maxPixels = 40;

            // Width and height.
            int originalWidth = original.Width;
            int originalHeight = original.Height;

            // Compute best factor to scale entire image based on larger dimension.
            double factor;
            if (originalWidth > originalHeight)
            {
                factor = (double)maxPixels / originalWidth;
            }
            else
            {
                factor = (double)maxPixels / originalHeight;
            }

            // Return thumbnail size.
            return new Size((int)(originalWidth * factor), (int)(originalHeight * factor));
        }


        public static byte[] ImageToByteArray(Image image)
        {
            if (image.DrawingImage != null)
                return ImageToByteArray(image.DrawingImage);
            else return null;
        }

        public static byte[] ImageToByteArray(System.Drawing.Image image, Action<Exception> logAction = null)
        {
            byte[] array = null;

            try
            {
                using (var ms = new MemoryStream())
                {
                    image.Save(ms, image.RawFormat);
                    array = ms.ToArray();
                }
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }

            return array;
        }

        public static string ImageToBase64String(Image image)
        {
            if (image.DrawingImage != null)
                return ImageToBase64String(image);
            else return null;
        }

        public static string ImageToBase64String(System.Drawing.Image image, Action<Exception> logAction = null)
        {
            string base64String = null;

            try
            {
                using (var ms = new MemoryStream())
                {
                    image.Save(ms, image.RawFormat);

                    var imageBytes = ms.ToArray();

                    base64String = Convert.ToBase64String(imageBytes);
                }
            }
            catch (Exception ex)
            {
                if (logAction != null)
                    logAction(ex);
            }

            return base64String;
        }

        public static string[] ConvertTiffToJpeg(string fileName)
        {
            using (System.Drawing.Image imageFile = System.Drawing.Image.FromFile(fileName))
            {
                FrameDimension frameDimensions = new FrameDimension(
                    imageFile.FrameDimensionsList[0]);

                // Gets the number of pages from the tiff image (if multipage) 
                int frameNum = imageFile.GetFrameCount(frameDimensions);
                string[] jpegPaths = new string[frameNum];

                for (int frame = 0; frame < frameNum; frame++)
                {
                    // Selects one frame at a time and save as jpeg. 
                    imageFile.SelectActiveFrame(frameDimensions, frame);
                    using (Bitmap bmp = new Bitmap(imageFile))
                    {
                        jpegPaths[frame] = String.Format("{0}\\{1}{2}.jpg",
                            System.IO.Path.GetDirectoryName(fileName),
                            System.IO.Path.GetFileNameWithoutExtension(fileName),
                            (frameNum == 1 ? string.Empty : frame.ToString()));

                        bmp.Save(jpegPaths[frame], ImageFormat.Jpeg);
                    }
                }

                return jpegPaths;
            }
        }

        #endregion
    }
}

using System.IO;
using System.IO.Compression;

namespace Veeam.GZipTest
{
    public class Compressor : Processor
    {
        public static void Run(string fileIn, string fileOut)
        {
            using var compressor = new Compressor(fileIn, fileOut);
            compressor.Run();
        }

        private uint blockCounter = 0;

        private Compressor(string fileIn, string fileOut) : base(fileIn, fileOut) { }

        protected override (uint, byte[]) ReadBlock(BinaryReader reader)
        {
            var data = reader.ReadBytes(blockSize);
            return (blockCounter++, data);
        }

        protected override byte[] ModifyData(byte[] data)
        {
            using var streamOut = new MemoryStream();
            using var streamIn = new GZipStream(streamOut, CompressionMode.Compress, true);

            streamIn.Write(data, 0, data.Length);
            streamIn.Close();

            streamOut.Seek(0, SeekOrigin.Begin);
            return streamOut.ToArray();
        }

        protected override void WriteBlock(BinaryWriter writer, uint index, byte[] data)
        {
            writer.Write(index);
            writer.Write(data.Length);
            writer.Write(data);
        }
    }
}

using System.IO;
using System.IO.Compression;

namespace Veeam.GZipTest
{
    public class Decompressor: Processor
    {
        public static void Run(string fileIn, string fileOut)
        {
            using var decompressor = new Decompressor(fileIn, fileOut);
            decompressor.Run();
        }

        private Decompressor(string fileIn, string fileOut) : base(fileIn, fileOut) { }

        protected override (uint, byte[]) ReadBlock(BinaryReader reader)
        {
            uint index = reader.ReadUInt32();
            int length = reader.ReadInt32();
            byte[] data = reader.ReadBytes(length);
            return (index, data);
        }

        protected override byte[] ModifyData(byte[] data)
        {
            using var streamIn = new MemoryStream(data);
            using var streamGZip = new GZipStream(streamIn, CompressionMode.Decompress);
            using var streamOut = new MemoryStream();

            streamGZip.CopyTo(streamOut);
            return streamOut.ToArray();
        }

        protected override void WriteBlock(BinaryWriter writer, uint index, byte[] data)
        {
            long offset = (long)index * BlockSize;
            writer.BaseStream.Seek(offset, SeekOrigin.Begin);
            writer.Write(data);
        }
    }
}

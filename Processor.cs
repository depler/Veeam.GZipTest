using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;

namespace Veeam.GZipTest
{
    public abstract class Processor: IDisposable
    {
        protected const int blockSize = 1024 * 1024;

        private readonly Stream streamIn;
        private readonly Stream streamOut;

        private readonly int threadsLimit;
        private readonly BlockingCollection<(uint index, byte[] data)> blocksIn;
        private readonly BlockingCollection<(uint index, byte[] data)> blocksOut;

        protected Processor(string fileIn, string fileOut)
        {
            streamIn = File.Open(fileIn, FileMode.Open);
            streamOut = File.Open(fileOut, FileMode.Create);

            threadsLimit = Environment.ProcessorCount;
            blocksIn = new BlockingCollection<(uint, byte[])>(threadsLimit * 2);
            blocksOut = new BlockingCollection<(uint, byte[])>(threadsLimit * 2);
        }

        protected abstract (uint index, byte[] data) ReadBlock(BinaryReader reader);
        protected abstract byte[] ModifyData(byte[] data);
        protected abstract void WriteBlock(BinaryWriter writer, uint index, byte[] data);

        public void Dispose()
        {
            blocksIn?.Dispose();
            blocksOut?.Dispose();
            streamIn?.Dispose();
            streamOut?.Dispose();
        }

        protected void Run()
        {
            var modThreads = Enumerable.Range(0, threadsLimit).Select(x => new Thread(ModifyBlocks)).ToArray();
            foreach (var modThread in modThreads)
                modThread.Start();

            var writerThread = new Thread(WriteBlocks);
            writerThread.Start();

            ReadBlocks();

            blocksIn.CompleteAdding();
            foreach (var modThread in modThreads)
                modThread.Join();

            blocksOut.CompleteAdding();
            writerThread.Join();
        }

        private void ReadBlocks()
        {
            using var reader = new BinaryReader(streamIn);

            while (streamIn.Position < streamIn.Length)
            {
                var block = ReadBlock(reader);
                blocksIn.Add(block);
            }
        }

        private void ModifyBlocks()
        {
            foreach (var blockIn in blocksIn.GetConsumingEnumerable())
            {
                var blockOut = (blockIn.index, ModifyData(blockIn.data));
                blocksOut.Add(blockOut);
            }
        }

        private void WriteBlocks()
        {
            using var writer = new BinaryWriter(streamOut);

            foreach (var blockOut in blocksOut.GetConsumingEnumerable())
                WriteBlock(writer, blockOut.index, blockOut.data);
        }
    }
}

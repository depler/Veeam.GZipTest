using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;

namespace Veeam.GZipTest
{
    public abstract class Processor : IDisposable
    {
        protected const int blockSize = 1024 * 1024;

        private readonly Stream streamIn;
        private readonly Stream streamOut;

        private readonly int threadsLimit;
        private readonly CancellationTokenSource ctsError;
        private readonly BlockingCollection<(uint index, byte[] data)> blocksIn;
        private readonly BlockingCollection<(uint index, byte[] data)> blocksOut;
        private readonly ConcurrentQueue<Exception> exceptions;

        protected Processor(string fileIn, string fileOut)
        {
            streamIn = File.Open(fileIn, FileMode.Open);
            streamOut = File.Open(fileOut, FileMode.Create);

            threadsLimit = Environment.ProcessorCount;
            ctsError = new CancellationTokenSource();
            blocksIn = new BlockingCollection<(uint, byte[])>(threadsLimit * 2);
            blocksOut = new BlockingCollection<(uint, byte[])>(threadsLimit * 2);
            exceptions = new ConcurrentQueue<Exception>();
        }

        protected abstract (uint index, byte[] data) ReadBlock(BinaryReader reader);
        protected abstract byte[] ModifyData(byte[] data);
        protected abstract void WriteBlock(BinaryWriter writer, uint index, byte[] data);

        public void Dispose()
        {
            blocksIn?.Dispose();
            blocksOut?.Dispose();
            ctsError?.Dispose();
            streamIn?.Dispose();
            streamOut?.Dispose();
        }

        protected void Run()
        {
            var readerThread = StartThread(ReadBlocks);
            var modifierThreads = Enumerable.Range(0, threadsLimit).Select(x => StartThread(ModifyBlocks)).ToArray();
            var writerThread = StartThread(WriteBlocks);

            readerThread.Join();
            blocksIn.CompleteAdding();

            foreach (var modifierThread in modifierThreads)
                modifierThread.Join();

            blocksOut.CompleteAdding();
            writerThread.Join();

            if (!exceptions.IsEmpty)
                throw new AggregateException(exceptions);
        }

        private Thread StartThread(Action action)
        {
            var thread = new Thread(() =>
            {
                try
                {
                    action();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                    ctsError.Cancel();
                }
            });

            thread.Start();
            return thread;
        }

        private void ReadBlocks()
        {
            using var reader = new BinaryReader(streamIn);

            while (streamIn.Position < streamIn.Length)
            {
                var block = ReadBlock(reader);
                blocksIn.Add(block, ctsError.Token);
            }
        }

        private void ModifyBlocks()
        {
            foreach (var blockIn in blocksIn.GetConsumingEnumerable(ctsError.Token))
            {
                var blockOut = (blockIn.index, ModifyData(blockIn.data));
                blocksOut.Add(blockOut, ctsError.Token);
            }
        }

        private void WriteBlocks()
        {
            using var writer = new BinaryWriter(streamOut);

            foreach (var blockOut in blocksOut.GetConsumingEnumerable(ctsError.Token))
                WriteBlock(writer, blockOut.index, blockOut.data);
        }
    }
}

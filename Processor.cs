using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading;

namespace Veeam.GZipTest
{
    public abstract class Processor : IDisposable
    {
        protected const int BlockSize = 1024 * 1024;

        private readonly Stream _streamIn;
        private readonly Stream _streamOut;

        private readonly int _threadsLimit;
        private readonly CancellationTokenSource _ctsError;
        private readonly BlockingCollection<(uint index, byte[] data)> _blocksIn;
        private readonly BlockingCollection<(uint index, byte[] data)> _blocksOut;
        private readonly ConcurrentQueue<Exception> _exceptions;

        protected Processor(string fileIn, string fileOut)
        {
            _streamIn = File.Open(fileIn, FileMode.Open);
            _streamOut = File.Open(fileOut, FileMode.Create);

            _threadsLimit = Environment.ProcessorCount;
            _ctsError = new CancellationTokenSource();
            _blocksIn = new BlockingCollection<(uint, byte[])>(_threadsLimit * 2);
            _blocksOut = new BlockingCollection<(uint, byte[])>(_threadsLimit * 2);
            _exceptions = new ConcurrentQueue<Exception>();
        }

        protected abstract (uint index, byte[] data) ReadBlock(BinaryReader reader);
        protected abstract byte[] ModifyData(byte[] data);
        protected abstract void WriteBlock(BinaryWriter writer, uint index, byte[] data);

        public void Dispose()
        {
            _blocksIn?.Dispose();
            _blocksOut?.Dispose();
            _ctsError?.Dispose();
            _streamIn?.Dispose();
            _streamOut?.Dispose();

            GC.SuppressFinalize(this);
        }

        protected void Run()
        {
            Thread readerThread = StartThread(ReadBlocks);
            Thread[] modifierThreads = Enumerable.Range(0, _threadsLimit).Select(x => StartThread(ModifyBlocks)).ToArray();
            Thread writerThread = StartThread(WriteBlocks);

            readerThread.Join();

            _blocksIn.CompleteAdding();
            foreach (Thread modifierThread in modifierThreads)
                modifierThread.Join();

            _blocksOut.CompleteAdding();
            writerThread.Join();

            if (!_exceptions.IsEmpty)
                throw new AggregateException(_exceptions);
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
                    _exceptions.Enqueue(ex);
                    _ctsError.Cancel();
                }
            });

            thread.Start();
            return thread;
        }

        private void ReadBlocks()
        {
            using var reader = new BinaryReader(_streamIn);

            while (_streamIn.Position < _streamIn.Length)
            {
                (uint index, byte[] data) block = ReadBlock(reader);
                _blocksIn.Add(block, _ctsError.Token);
            }
        }

        private void ModifyBlocks()
        {
            foreach ((uint index, byte[] data) in _blocksIn.GetConsumingEnumerable(_ctsError.Token))
            {
                byte[] modifiedData = ModifyData(data);
                _blocksOut.Add((index, modifiedData), _ctsError.Token);
            }
        }

        private void WriteBlocks()
        {
            using var writer = new BinaryWriter(_streamOut);

            foreach ((uint index, byte[] data) in _blocksOut.GetConsumingEnumerable(_ctsError.Token))
                WriteBlock(writer, index, data);
        }
    }
}

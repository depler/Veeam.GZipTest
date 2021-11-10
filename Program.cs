using System;

namespace Veeam.GZipTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                if (args.Length != 3)
                {
                    Console.WriteLine("Usage: GZipTest.exe [compress|decompress] [source file] [target file]");
                    return;
                }

                switch (args[0])
                {
                    case "compress":
                        {
                            Console.WriteLine($"Compressing file {args[1]}...");
                            Compressor.Run(args[1], args[2]);
                            break;
                        }
                    case "decompress":
                        {
                            Console.WriteLine($"Decompressing file {args[1]}...");
                            Decompressor.Run(args[1], args[2]);
                            break;
                        }
                    default: throw new Exception($"Unknown command: {args[0]}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
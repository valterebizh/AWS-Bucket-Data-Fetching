using System;

namespace FetchDataFromAWS
{
    class Program
    {

        static void Main(string[] args)
        {
            
            
            Fetcher fetcher = new Fetcher();
            Console.WriteLine(fetcher.FetchFolders());
        }
    }
}

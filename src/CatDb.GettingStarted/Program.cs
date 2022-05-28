﻿using DatabaseBenchmark;
using CatDb.Data;
using CatDb.Database;
using CatDb.General.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.IO;
using System.Text;
using CatDb.Storage;
using System.Globalization;
using System.Threading;
using CatDb.WaterfallTree;
using CatDb.General.Collections;
using CatDb.General.Comparers;

namespace CatDb.GettingStarted
{
    class Program
    {
        static void Main(string[] args)
        {
            Example(1000000, KeysType.Random);

            Console.ReadKey();
        }

        private static void Example(int tickCount, KeysType keysType)
        {
            Stopwatch sw = new Stopwatch();
            const string FILE_NAME = "test.CatDb";
            File.Delete(FILE_NAME);

            //insert
            Console.WriteLine("Inserting...");
            sw.Reset();
            sw.Start();
            int c = 0;
            using (IStorageEngine engine = Database.CatDb.FromFile(FILE_NAME))
            {
                ITable<long, Tick> table = engine.OpenXTable<long, Tick>("table");

                foreach (var kv in TicksGenerator.GetFlow(tickCount, keysType)) //generate random records
                {
                    table[kv.Key] = kv.Value;

                    c++;
                    if (c % 100000 == 0)
                        Console.WriteLine("Inserted {0} records", c);
                }

                var table2 = engine.OpenXTable<string, string>("table2");
                table2["My Random Key"] = "Random Value";
                table2["My Random Key2"] = "Random Value2";

                engine.Commit();
            }
            sw.Stop();
            Console.WriteLine("Insert speed:{0} rec/sec", sw.GetSpeed(tickCount));

            //read
            Console.WriteLine("Reading...");
            sw.Reset();
            sw.Start();
            c = 0;
            using (IStorageEngine engine = Database.CatDb.FromFile(FILE_NAME))
            {
                ITable<long, Tick> table = engine.OpenXTable<long, Tick>("table");

                foreach (var row in table) //table.Forward(), table.Backward()
                {
                    //Console.WriteLine("{0} {1}", row.Key, row.Value);

                    c++;
                    if (c % 100000 == 0)
                        Console.WriteLine("Read {0} records", c);
                }

                var table2 = engine.OpenXTable<string, string>("table2");

                Console.WriteLine($"{table2["My Random Key"]}");
                Console.WriteLine($"{table2["My Random Key2"]}");
            }
            sw.Stop();
            Console.WriteLine("Read speed:{0} records", sw.GetSpeed(c));
        }
    }
}

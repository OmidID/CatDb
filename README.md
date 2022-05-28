# CatDb

CatDb is a NoSQL key-value store open-source database with innovative indexing algorithm. The CatDb engine is based on WaterfallTree technology which provides blazing performance in real-time indexing of both sequential and random keys, making CatDb perfect for BigData and enterprise systems.

CatDb forked from STSdb 4.0. Ported to .Net 6 and try to make it more compatible with multi-platform in future. The main idea is to get rid of code injection to be able to use it everywhere.

# Installation

| Name | Description | Version |
|:-|:-|:-|
| [CatDb](https://www.nuget.org/packages/CatDb/) | Database engine library | ![Nuget](https://badgen.net/nuget/v/CatDb) |


# Key Features

## Innovative Technology
The storage engine of CatDb is based on an innovative data indexing structure called WaterfallTree. WaterfallTree is an algorithm that effectively solves one of the fundamental problems in the database world – speed degradation when indexing random keys.

More about WaterfallTree: https://ieeexplore.ieee.org/document/6857846/references.

## Performance
CatDb provides up to 100x increase in indexing speed and data processing.

* up to 6x increase compared to LSM-tree technology.
* up to 10x increase compared to FractalTree technology.
* up to 100x increase compared to B-tree technology.
 
# Compression
CatDb is not only faster, but more compact in size. In most of the cases it can achieve up to 4x better compression than competitive solutions thanks to fast parallel vertical compressions.

## BigData
With its innovative WaterfallTree technology, CatDb is the perfect choice for BigData. CatDb can be used as a scalable and versatile node for cloud computing and enterprise systems. 

## Usage

```csharp
var engine = Database.CatDb.FromFile(FILE_NAME);
var table = engine.OpenXTable<long, Tick>("table");
var table2 = engine.OpenXTable<string, string>("table2");

table2["My Random Key"] = "Random Value";
table2["My Random Key2"] = "Random Value2";

//Save to file
engine.Commit();

Console.WriteLine(table2["My Random Key"]);
Console.WriteLine(table2["My Random Key2"]);
```

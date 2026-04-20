using CatDb.Data;
using CatDb.General.Persist;

namespace CatDb.WaterfallTree;

public interface IDescriptor
{
    long    Id            { get; }
    string? Name          { get; }
    int    StructureType { get; }

    /// Describes the key type.
    DataType KeyDataType { get; }

    /// Describes the record type.
    DataType RecordDataType { get; }

    /// Can be anonymous or user type.
    Type? KeyType    { get; set; }

    /// Can be anonymous or user type.
    Type? RecordType { get; set; }

    IComparer<IData>?          KeyComparer           { get; set; }
    IEqualityComparer<IData>?  KeyEqualityComparer   { get; set; }
    IPersist<IData>?           KeyPersist            { get; set; }
    IPersist<IData>?           RecordPersist         { get; set; }
    IIndexerPersist<IData>?    KeyIndexerPersist     { get; set; }
    IIndexerPersist<IData>?    RecordIndexerPersist  { get; set; }

    DateTime CreateTime   { get; }
    DateTime ModifiedTime { get; }
    DateTime AccessTime   { get; }

    byte[]? Tag { get; set; }
}
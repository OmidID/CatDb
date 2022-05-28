﻿namespace CatDb.General.Persist
{
    public interface IIndexerPersist
    {
    }

    public interface IIndexerPersist<T> : IIndexerPersist
    {
        void Store(BinaryWriter writer, Func<int, T> values, int count);
        void Load(BinaryReader reader, Action<int, T> values, int count);
    }
}

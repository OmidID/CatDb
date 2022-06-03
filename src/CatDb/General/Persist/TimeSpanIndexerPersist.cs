namespace CatDb.General.Persist
{
    public class TimeSpanIndexerPersist : IIndexerPersist<TimeSpan>
    {
        private const byte VERSION = 40;

        private static readonly long Millisecond = 10000;
        private static readonly long Second = 1000 * Millisecond;
        private static readonly long Minute = 60 * Second;
        private static readonly long Hour = 60 * Minute;
        private static readonly long Day = 24 * Hour;

        private readonly Int64IndexerPersist _persist = new(new[] { Millisecond, Second, Minute, Hour, Day });

        public void Store(BinaryWriter writer, Func<int, TimeSpan> values, int count)
        {
            writer.Write(VERSION);

            _persist.Store(writer, i => { return values(i).Ticks; }, count);
        }

        public void Load(BinaryReader reader, Action<int, TimeSpan> values, int count)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid DateTimeIndexerPersist version.");

            _persist.Load(reader, (i, v) => { values(i, new TimeSpan(v)); }, count);
        }
    }
}
